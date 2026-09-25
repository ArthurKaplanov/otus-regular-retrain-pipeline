import mlflow
import mlflow.spark
from loguru import logger
from pyspark.ml import Pipeline
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
from pyspark.sql.utils import AnalysisException

from regular_retrain.config import (
    APP_NAME,
    AWS_SDK_VERSION,
    DATA_W_FEATURES_PATH,
    HADOOP_VERSION,
    YC_ACCESS_KEY,
    YC_SECRET_KEY,
)


def create_spark_session(
        app_name:str=APP_NAME,
        yc_access_key=YC_ACCESS_KEY,
        yc_secret_key=YC_SECRET_KEY
        ) -> SparkSession.Builder:
    """func to create a spark builder

    Args:
        app_name (str, optional): _description_. Defaults to APP_NAME.
        yc_access_key (_type_, optional): _description_. Defaults to YC_ACCESS_KEY.
        yc_secret_key (_type_, optional): _description_. Defaults to YC_SECRET_KEY.

    Returns:
        SparkSession.Builder: создаем builder
    """
    spark = (
         SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config(
                "spark.jars.packages",
                f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION},com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
            .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
            .config("spark.hadoop.fs.s3a.access.key", yc_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", yc_secret_key)
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
            .config("spark.driver.memory", "3g")
            )
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


def split_data(data:DataFrame) -> tuple[DataFrame, DataFrame]:
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    return train, test

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
    best_dt = best_model.stages[-1]
    return best_model, best_dt


def register_model(experiment_name):
    """Register a new model if it's not existing

    Args:
        experiment_name (_type_): name of experiment
    """
    def check_model_is_exist(client: mlflow.MlflowClient, model_name) -> bool:
        """ check the model is existing
        Returns:
            bool: _description_
        """
        try:
            logger.info(f"Проверяем существует ли модель {model_name}")
            client.get_registered_model(model_name)
            logger.info(f"Модель '{model_name}' уже зарегистрирована")
            return True
        except Exception as e:
            logger.info(f"Создаем новую модель: {str(e)}")
            logger.info(f"Создана новая регистрированная модель '{model_name}'")
            return False

    print(f"DEBUG: Сравниваем и регистрируем модель для эксперимента {experiment_name}")
    client = mlflow.MlflowClient()

    # Имя модели
    model_name = f"{experiment_name}_model"
    print(f"DEBUG: Имя модели: {model_name}")

    if not check_model_is_exist(client, model_name):
        client.create_registered_model(model_name)

def main():

    tracking_uri = "http://localhost:5005/"
    experiment_name = "fraud_detections"

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    logger.info("Start session")
    spark = create_spark_session(APP_NAME,YC_ACCESS_KEY,YC_SECRET_KEY).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Data reading is coming")
    df = spark.read.parquet(DATA_W_FEATURES_PATH)
    logger.info("Data was read successfully")

    logger.info("Calculating class weights...")
    weight_for_legit, weight_for_fraud = get_classes_weight(df)
    df = df.withColumn(
        "class_weight",
        f.when(f.col("tx_fraud") == 1.0, weight_for_fraud).otherwise(weight_for_legit),
    )

    logger.info("Start creating a new feature for dataframe")
    df = create_relative_features(df)
    logger.info("Finished creating new features for dataframe")

    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="tx_fraud",
        predictionCol="prediction",
        metricName="recallByLabel",
        metricLabel=1.0,
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="tx_fraud",
        rawPredictionCol="probability",  # используем вектор вероятностей
        metricName="areaUnderPR",  # лучшая метрика для фрода
    )


    with mlflow.start_run(run_name="fraud_detection_model"):

        train, test = split_data(df)
        best_model, best_dt = train_spark_model(train, evaluator)

        logger.info("Best maxDepth:", best_dt.getMaxDepth())
        mlflow.log_params({"Best maxDepth": best_dt.getMaxDepth()})

        test_predictions = best_model.transform(test)

        test_pr_auc = evaluator.evaluate(test_predictions)
        test_recall = evaluator_recall.evaluate(test_predictions)

        mlflow.log_metrics({"PR AUC":test_pr_auc, "Recall":test_recall})
        logger.info(f"\nTest PR AUC: {test_pr_auc:.4f}")
        logger.info(f"Test recall (at default 0.5 threshold): {test_recall:.4f}")

        mlflow.spark.log_model(best_model, "model")

        register_model(experiment_name)


if __name__ == "__main__":
    main()
