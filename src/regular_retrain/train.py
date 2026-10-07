from __future__ import annotations
import os
import argparse
from datetime import datetime
from datetime import timedelta

import mlflow
import mlflow.spark
import numpy as np
from loguru import logger
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    StandardScaler,
    VectorAssembler,
)
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import percent_rank
from pyspark.sql.utils import AnalysisException # type: ignore


def create_spark_session(
        app_name:str
        ) -> SparkSession.Builder:
    """func to create a spark builder

    Args:
        app_name (str, optional): _description_. Defaults to APP_NAME.
        yc_access_key (_type_, optional): _description_. Defaults to YC_ACCESS_KEY.
        yc_secret_key (_type_, optional): _description_. Defaults to YC_SECRET_KEY.

    Returns:
        SparkSession.Builder: создаем builder
    """
    spark = SparkSession.builder.appName(app_name)
    return spark

def get_classes_weight(data: DataFrame, fraud_class:float=1.0) -> tuple:
    """ РАСЧЕТ ВЕСОВ КЛАССОВ (Борьба с дисбалансом)

    Args:
        data (DataFrame): _description_
        fraud_class (float, optional): _description_. Defaults to 1.0.

    Returns:
        tuple: _description_
    """
    total_count = data.count()
    frauds_count = data.filter(f.col("tx_fraud") == fraud_class).count()
    legit_count = total_count - frauds_count

    weight_for_legit = total_count / (2.0 * legit_count)
    weight_for_fraud = total_count / (2.0 * frauds_count)
    return weight_for_legit, weight_for_fraud


def create_relative_features(data: DataFrame) -> DataFrame:
    """ГЕНЕРАЦИЯ ОТНОСИТЕЛЬНЫХ ФИЧ (Борьба со слепотой модели)
    # 1. Отношение суммы текущей транзакции к среднему чеку клиента за день
    # 2. Доля трат за последние 5 минут от всех дневных трат клиента
    # 3. Плотность транзакций: какая доля дневных операций пришлась на последний час
    # 4. Интенсивность смены терминалов: сколько уникальных точек приходится
    # на одну транзакцию за 5 минут
    Args:
        data (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    need_columns = {
        'tx_amount',
        'customer_avg_amount_today',
        'customer_amount_sum_5m',
        'customer_amount_sum_today',
        'customer_tx_count_1h',
        'customer_tx_count_today',
        'customer_unique_terminals_5m',
        'customer_tx_count_5m'
    }
    existed_columns = set(data.columns)
    if not need_columns.issubset(existed_columns):
        lost_columns = need_columns.difference(existed_columns)
        raise AnalysisException(f"We can't find the next columns {lost_columns}")


    df = data.withColumn(
    "ratio_amount_to_avg_today",
    f.col("tx_amount") / (f.col("customer_avg_amount_today") + 1.0),
    )
    df = df.withColumn(
        "ratio_amount_5m_to_today",
        f.col("customer_amount_sum_5m") / (f.col("customer_amount_sum_today") + 1.0),
    )
    df = df.withColumn(
        "ratio_count_1h_to_today",
        f.col("customer_tx_count_1h") / (f.col("customer_tx_count_today") + 1.0),
    )
    df = df.withColumn(
        "ratio_terminals_per_tx_5m",
        f.col("customer_unique_terminals_5m") / (f.col("customer_tx_count_5m") + 1.0),
    )
    return df


def split_data(
    data: DataFrame,
    datetime_col: str = "tx_datetime",
    validation_days: int = 7,
    test_days: int = 7,
) -> tuple[DataFrame, DataFrame, DataFrame]:

    max_datetime = (
        data
        .agg(f.max(datetime_col).alias("max_datetime"))  # type: ignore
        .first()["max_datetime"]
    )

    if max_datetime is None:
        raise ValueError("Dataset is empty")

    test_start = max_datetime - timedelta(days=test_days)
    validation_start = test_start - timedelta(days=validation_days)

    train = data.filter(
        f.col(datetime_col) < f.lit(validation_start)
    )

    validation = data.filter(
        (f.col(datetime_col) >= f.lit(validation_start))
        & (f.col(datetime_col) < f.lit(test_start))
    )

    test = data.filter(
        f.col(datetime_col) >= f.lit(test_start)
    )

    return train, validation, test


def find_threshold(
    validation_data: DataFrame,
    eval_recall: MulticlassClassificationEvaluator,
    eval_fpr: MulticlassClassificationEvaluator,
    max_fpr: float = 0.05,
) -> tuple[float, float, float]:

    validation_data = (
        validation_data
        .withColumn(
            "fraud_score",
            vector_to_array("probability")[1],  # type: ignore
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        for threshold in np.arange(0.1, 0.91, 0.05):
            threshold = float(threshold)

            predictions = validation_data.withColumn(
                "prediction",
                f.when(
                    f.col("fraud_score") >= threshold,
                    1.0,
                ).otherwise(0.0),
            )

            false_positive_rate = eval_fpr.evaluate(predictions)

            if false_positive_rate <= max_fpr:
                recall = eval_recall.evaluate(predictions)

                return (
                    threshold,
                    recall,
                    false_positive_rate,
                )

        raise ValueError(
            f"No threshold satisfies FPR <= {max_fpr}"
        )

    finally:
        validation_data.unpersist()

def evaluate_threshold(
        test_predictions: DataFrame,
        threshold:float,
        eval_recall: MulticlassClassificationEvaluator,
        eval_fpr: MulticlassClassificationEvaluator
    ) -> tuple[float, float]:
    test_data = (
        test_predictions
        .withColumn("fraud_score",
                    vector_to_array(f.col("probability"))[1]
                    )
        )

    predictions = test_data.withColumn(
        "prediction",
        f.when(
            f.col("fraud_score") >= threshold,
            1.0,
        ).otherwise(0.0),
    )
    return eval_recall.evaluate(predictions), eval_fpr.evaluate(predictions)


def train_spark_model(train_data:DataFrame, evaluator):

    numeric_assembler = VectorAssembler(
        inputCols=[
            # --- НОВЫЕ ОТНОСИТЕЛЬНЫЕ ФИЧИ (Сигналы аномалий) ---
            "ratio_amount_to_avg_today",
            "ratio_amount_5m_to_today",
            "ratio_count_1h_to_today",
            "ratio_terminals_per_tx_5m",
            # ----------------------------------------------------
            # Базовые фичи транзакции
            "tx_amount",
            "tx_time_seconds",
            "tx_time_days",
            # Исходные 24-часовые агрегаты по клиенту
            "customer_amount_sum_today",
            "customer_tx_count_today",
            "customer_avg_amount_today",
            # Velocity-фичи: Количество транзакций (5 минут и 1 час)
            "customer_tx_count_5m",
            "customer_tx_count_1h",
            # Velocity-фичи: Суммы транзакций (5 минут и 1 час)
            "customer_amount_sum_5m",
            "customer_amount_sum_1h",
            # Уникальные терминалы за разные окна
            "customer_unique_terminals_5m",
            "customer_unique_terminals_1h",
            "customer_unique_terminals_24h",
            # Разнообразие терминалов (Diversity)
            "customer_terminal_diversity_5m",
            "customer_terminal_diversity_1h",
            # История и временные интервалы между транзакциями
            "customer_has_prev_tx",
            "customer_seconds_since_prev_tx",
            # Циклические фичи времени
            "hour_sin",
            "hour_cos",
        ],
        outputCol="numeric_features",
        handleInvalid="skip",
    )

    scaler = StandardScaler(
        inputCol="numeric_features",
        outputCol="features",
        withStd=True,
        withMean=True,
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="tx_fraud",
        weightCol="class_weight",
        numTrees=35,
        seed=42,
    )

    pipeline_rf = Pipeline(
        stages=[
            numeric_assembler,
            scaler,
            rf,
        ]
    )

    grid_rf = ParamGridBuilder().addGrid(rf.maxDepth, [3, 7]).build()

    tvs = TrainValidationSplit(
        estimator=pipeline_rf,
        estimatorParamMaps=grid_rf,
        evaluator=evaluator,
        trainRatio=0.8,
        parallelism=1,
        seed=42,
    )

    logger.info("Starting training/validation process")
    tvsModel = tvs.fit(train_data)
    logger.info("Finishing training/validation process")

    best_model = tvsModel.bestModel
    best_dt = best_model.stages[-1] # type: ignore
    return best_model, best_dt


def main():
    # Параметры запуска отделяют код от конкретного окружения.
    # Airflow сможет передать другой датасет или адрес MLflow,
    # не меняя сам train.py.
    parser = argparse.ArgumentParser(
        description="Fraud Detection Model Training"
    )

    parser.add_argument("--input", required=True, help="Input Parquet path")
    parser.add_argument("--app-name", default="FraudDetectionModel")

    # Tracking URI — адрес сервера MLflow.
    # При HTTP-подключении именно сервер работает с PostgreSQL.
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", default="fraud_detection")

    # action="store_true": флаг присутствует — True, отсутствует — False.
    parser.add_argument("--auto-register", action="store_true")
    parser.add_argument("--run-name", default=None)

    parser.add_argument("--s3-endpoint-url")
    parser.add_argument("--s3-access-key")
    parser.add_argument("--s3-secret-key")

    args = parser.parse_args()

    # Проверяем, что переданы либо оба ключа, либо ни одного.
    # parser.error() завершит процесс с ненулевым кодом.
    if bool(args.s3_access_key) != bool(args.s3_secret_key):
        parser.error(
            "--s3-access-key and --s3-secret-key must be provided together"
        )

    # Это настройки Python-клиента MLflow для прямой загрузки
    # артефактов в S3. Доступ Spark к данным через Hadoop S3A
    # настраивается отдельно, в окружении Data Proc.
    if args.s3_endpoint_url:
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint_url

    # Если ключи не переданы через CLI, существующее окружение не меняем.
    # CLI-ключи — временный вариант: аргументы могут быть видны
    # в описании задания.
    if args.s3_access_key and args.s3_secret_key:
        os.environ["AWS_ACCESS_KEY_ID"] = args.s3_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = args.s3_secret_key

    # Имя run может повторяться: уникальность обеспечивает run_id.
    run_name = args.run_name or "fraud_detection_rf"

    # FPR = FP / (FP + TN): доля легитимных транзакций,
    # ошибочно помеченных как fraud.
    max_fpr = 0.05

    # Если создание сессии упадёт, finally увидит None
    # и не станет останавливать несуществующую сессию.
    spark = None

    logger.info(
        "Training: input={}, experiment={}, run_name={}",
        args.input,
        args.experiment_name,
        run_name,
    )

    try:
        mlflow.set_tracking_uri(args.tracking_uri)

        # Experiment объединяет связанные запуски обучения.
        # set_experiment() выберет существующий или создаст новый.
        mlflow.set_experiment(args.experiment_name)

        # Один run охватывает чтение, обучение, оценку и сохранение.
        # При обычном выходе статус станет FINISHED,
        # при выходе с исключением — FAILED.
        with mlflow.start_run(run_name=run_name) as run:
            logger.info("MLflow run started: {}", run.info.run_id)

            # Функция уже возвращает сессию: getOrCreate() здесь не нужен.
            # Внутри неё не должно быть master("local[*]"):
            # режим выполнения получаем из настроек отправки задания.
            spark = create_spark_session(app_name=args.app_name).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")

            logger.info(
                "Spark initialized: version={}, master={}, application_id={}",
                spark.version,
                spark.sparkContext.master,
                spark.sparkContext.applicationId,
            )

            # Теги описывают контекст запуска.
            # application_id связывает MLflow run с логами Spark/YARN.
            mlflow.set_tags({
                "spark_application_id": spark.sparkContext.applicationId,
                "spark_version": spark.version,
                "model_type": "random_forest",
            })

            # Параметры фиксируют настройки эксперимента.
            # Путь к данным полезен для поиска, но сам по себе
            # не является версией данных: содержимое пути может меняться.
            mlflow.log_params({
                "input_path": args.input,
                "max_fpr": max_fpr,
                "selection_metric": "areaUnderPR",
            })

            logger.info("Reading data and creating relative features")
            df = spark.read.parquet(args.input)

            # Эта подготовка находится вне сохраняемого Pipeline.
            # При inference такие же признаки нужно вычислить отдельно.
            df = create_relative_features(df)

            logger.info("Splitting data into train, validation and test")

            # В нашей функции разбиение идёт по времени:
            # train — прошлое, validation — следующий период,
            # test — последний период для финальной проверки.
            train, validation, test = split_data(df)

            logger.info("Calculating class weights")

            # Веса вычисляем только по train.
            # При fit() они меняют вклад классов в обучение.
            # Validation и test оцениваем без этих весов:
            # нас интересуют метрики на фактических транзакциях.
            weight_for_legit, weight_for_fraud = get_classes_weight(train)

            train = train.withColumn(
                "class_weight",
                f.when(
                    f.col("tx_fraud") == 1.0,
                    weight_for_fraud,
                ).otherwise(weight_for_legit),
            )

            # PR AUC оценивает качество ранжирования при разных порогах.
            # Это выбранный нами критерий подбора гиперпараметров.
            evaluator_pr_auc = BinaryClassificationEvaluator(
                labelCol="tx_fraud",
                rawPredictionCol="probability",
                metricName="areaUnderPR",
            )

            # metricLabel=1.0 означает, что считаем Recall именно fraud:
            # TP / (TP + FN), а не усреднённую метрику по классам.
            evaluator_recall = MulticlassClassificationEvaluator(
                labelCol="tx_fraud",
                predictionCol="prediction",
                metricName="recallByLabel",
                metricLabel=1.0,
            )

            evaluator_fpr = MulticlassClassificationEvaluator(
                labelCol="tx_fraud",
                predictionCol="prediction",
                metricName="falsePositiveRateByLabel",
                metricLabel=1.0,
            )

            logger.info("Training and selecting model by PR AUC")

            # Сохраняем текущую реализацию с TrainValidationSplit:
            # она подбирает параметры на внутреннем случайном
            # разбиении train. Внешние validation и test сюда не входят.
            #
            # best_model — весь PipelineModel;
            # best_rf — обученный Random Forest внутри него.
            best_model, best_rf = train_spark_model(
                train,
                evaluator_pr_auc,
            )

            logger.info("Selecting threshold on validation data")
            validation_predictions = best_model.transform(validation)

            # После выбора модели находим порог, удовлетворяющий
            # ограничению FPR на validation.
            # Порог определяет, какой score считать подозрительным.
            threshold, val_recall, val_fpr = find_threshold(
                validation_data=validation_predictions,
                eval_recall=evaluator_recall,
                eval_fpr=evaluator_fpr,
                max_fpr=max_fpr,
            )

            logger.info("Evaluating selected model and threshold on test data")
            test_predictions = best_model.transform(test)

            # На test используем уже выбранный порог.
            # По результатам test его не подстраиваем.
            # FPR здесь может превышать 5%: ограничение на validation
            # не гарантирует тот же результат на новых данных.
            test_recall, test_fpr = evaluate_threshold(
                test_predictions=test_predictions,
                threshold=threshold,
                eval_recall=evaluator_recall,
                eval_fpr=evaluator_fpr,
            )

            test_pr_auc = evaluator_pr_auc.evaluate(test_predictions)

            logger.info(
                "Selected model: maxDepth={}, threshold={:.4f}",
                best_rf.getMaxDepth(),
                threshold,
            )
            logger.info(
                "Validation: recall={:.4f}, fpr={:.4f}",
                val_recall,
                val_fpr,
            )
            logger.info(
                "Test: recall={:.4f}, fpr={:.4f}, pr_auc={:.4f}",
                test_recall,
                test_fpr,
                test_pr_auc,
            )

            # Записываем конфигурацию выбранной модели.
            # У RandomForestClassificationModel getNumTrees —
            # свойство, поэтому оно используется без скобок.
            mlflow.log_params({
                "best_max_depth": best_rf.getMaxDepth(),
                "num_trees": best_rf.getNumTrees,
                "seed": best_rf.getSeed(),
                "threshold": float(threshold),
                "weight_for_legit": weight_for_legit,
                "weight_for_fraud": weight_for_fraud,
            })

            # Метрики — измеренные результаты запуска.
            # Названия различают validation и независимую test-оценку.
            mlflow.log_metrics({
                "val_recall_at_fpr": val_recall,
                "val_fpr": val_fpr,
                "test_recall_at_selected_threshold": test_recall,
                "test_fpr_at_selected_threshold": test_fpr,
                "test_pr_auc": test_pr_auc,
            })

            # Подобранный threshold не встроен в best_model.
            # Сохраняем правило отдельно: при inference нужно взять
            # probability[1] и сравнить её с threshold через >=.
            #
            # Файл принадлежит run. При загрузке версии из Registry
            # его можно найти через связанный с этой версией run_id.
            mlflow.log_dict(
                {
                    "threshold": float(threshold),
                    "positive_class": 1,
                    "score_column": "probability",
                    "score_index": 1,
                    "comparison": ">=",
                    "max_validation_fpr": max_fpr,
                },
                "decision_policy.json",
            )

            # Флаг управляет созданием версии в Model Registry.
            # Регистрация версии не назначает её production-моделью
            # и сама по себе не проверяет допустимость test-метрик.
            registered_model_name = (
                f"{args.experiment_name}_model"
                if args.auto_register
                else None
            )

            logger.info("Saving model to MLflow")

            # Сохраняем весь PipelineModel: assembler, scaler и лес.
            # "model" — путь артефакта внутри run;
            # физическое хранилище определяется настройками MLflow.
            model_info = mlflow.spark.log_model(  # type: ignore
                spark_model=best_model,
                artifact_path="model",
                registered_model_name=registered_model_name,
            )

            logger.info("Model saved: {}", model_info.model_uri)

        # Сообщаем об успехе после выхода из with:
        # завершение run на сервере тоже должно пройти успешно.
        logger.info("Training run completed successfully")

    except Exception:
        # logger.exception() записывает traceback.
        # raise сохраняет ошибку: процесс завершится неуспешно,
        # и ожидающий результат задания Airflow сможет увидеть failure.
        logger.exception("Training job failed")
        raise

    finally:
        # finally выполняется при успехе и обычном исключении.
        # stop() освобождает ресурсы Spark-приложения,
        # но не удаляет сам Data Proc-кластер.
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception:
                # Ошибка очистки не должна скрыть исходную
                # причину падения обучения или сохранения модели.
                logger.opt(exception=True).warning(
                    "Could not stop Spark session cleanly"
                )


# При запуске файла выполняем main().
# При импорте файла из другого модуля обучение не запускается.
if __name__ == "__main__":
    main()
