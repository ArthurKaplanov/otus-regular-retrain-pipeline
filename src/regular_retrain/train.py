
import numpy as np
from loguru import logger
from regular_retrain.config import (
    AWS_SDK_VERSION,
    PROJECT_PATH,
    DATA_PATH,
    DATA_SAMPLE_PATH,
    DATA_MINI_SAMPLE_PATH,
    HADOOP_VERSION,
    YC_ACCESS_KEY,
    YC_SECRET_KEY,
)
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (
    StandardScaler,
    VectorAssembler,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from pyspark.sql.window import Window

logger.info("Start session")
spark = (
    SparkSession.builder \
    .appName("YandexCloudStorage") \
    .master("local[*]")\
    .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION},com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
    .config("spark.hadoop.fs.s3a.access.key", YC_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", YC_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()
)

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
    ]

)

spark.sparkContext.setLogLevel("WARN")

logger.info("Data reading is coming")
df = spark.read.parquet(DATA_PATH).filter("tx_datetime >= '2019-11-24 00:00:00'")
logger.info("Data was read succesfully ")
# ---

logger.info("Starting feature engineering")
SEC_5_MIN = 5 * 60
SEC_1_HOUR = 60 * 60
SEC_24_HOURS = 24 * 60 * 60
SEC_7_DAYS = 7 * 24 * 60 * 60
SEC_30_DAYS = 30 * 24 * 60 * 60

def get_customer_window(seconds):
    return (
        Window
        .partitionBy("customer_id")
        .orderBy("tx_time_seconds")
        .rangeBetween(-seconds, -1)
    )

w_customer_today = get_customer_window(SEC_24_HOURS)
w_terminal_today = (
    Window
    .partitionBy("terminal_id")
    .orderBy("tx_time_seconds")
    .rangeBetween(-SEC_24_HOURS, -1)
)

# Новые окна по клиенту на разные интервалы
w_cust_5m = get_customer_window(SEC_5_MIN)
w_cust_1h = get_customer_window(SEC_1_HOUR)
w_cust_7d = get_customer_window(SEC_7_DAYS)
w_cust_30d = get_customer_window(SEC_30_DAYS)


df_result = df.withColumns({
    # --- Ваши текущие фичи (исправленные на честный 24h range) ---
    "customer_amount_sum_today": f.sum("tx_amount").over(w_customer_today),
    "terminal_amount_sum_today": f.sum("tx_amount").over(w_terminal_today),
    "customer_tx_count_today": f.count("transaction_id").over(w_customer_today),
    "terminal_tx_count_today": f.count("transaction_id").over(w_terminal_today),
    "customer_avg_amount_today": f.avg("tx_amount").over(w_customer_today),
    "terminal_avg_amount_today": f.avg("tx_amount").over(w_terminal_today),

    # --- Новые Velocity Features: Количество транзакций ---
    "customer_tx_count_5m": f.count("transaction_id").over(w_cust_5m),
    "customer_tx_count_1h": f.count("transaction_id").over(w_cust_1h),
    "customer_tx_count_7d": f.count("transaction_id").over(w_cust_7d),

    # --- Новые Velocity Features: Сумма транзакций ---
    "customer_amount_sum_5m": f.sum("tx_amount").over(w_cust_5m),
    "customer_amount_sum_1h": f.sum("tx_amount").over(w_cust_1h),
    "customer_amount_sum_7d": f.sum("tx_amount").over(w_cust_7d),

    # --- Средний чек за 30 дней и сравнение ---
    "customer_avg_amount_30d": f.avg("tx_amount").over(w_cust_30d),
    # Отношение текущего чека к среднему за месяц (добавляем 1 костыль от деления на 0)
    "customer_current_vs_avg_30d": (
        f.when(
            (f.avg("tx_amount").over(w_cust_30d).isNull()) |
            (f.avg("tx_amount").over(w_cust_30d) == 0.0),
            f.lit(1.0) # Если истории нет или среднее равно 0, отношение будет 1.0 (или другое дефолтное значение)
        ).otherwise(
            f.col("tx_amount") / f.avg("tx_amount").over(w_cust_30d)
        )
    ),

    # Циклические фичи времени
    "hour_sin": f.sin(2 * 3.141592653589793 * f.hour("tx_datetime") / 24),
    "hour_cos": f.cos(2 * 3.141592653589793 * f.hour("tx_datetime") / 24),
})



# 5. Заполнение пустых значений (если истории за период нет, возвращаем 0)
fill_dict = {
    "customer_amount_sum_today": 0.0, "customer_avg_amount_today": 0.0,
    "terminal_amount_sum_today": 0.0, "terminal_avg_amount_today": 0.0,
    "customer_tx_count_5m": 0, "customer_tx_count_1h": 0, "customer_tx_count_7d": 0,
    "customer_amount_sum_5m": 0.0, "customer_amount_sum_1h": 0.0, "customer_amount_sum_7d": 0.0,
    "customer_avg_amount_30d": 0.0, "customer_current_vs_avg_30d": 1.0
}

df_result = df_result.fillna(fill_dict)
logger.info("Feature engineering is completed")
# ---

numeric_assembler = VectorAssembler(
    inputCols=[
        # Базовые фичи транзакции
        "tx_amount",

        # Ваши исходные фичи (честные 24-часовые агрегаты)
        "customer_amount_sum_today",
        "customer_tx_count_today",
        "customer_avg_amount_today",
        "terminal_amount_sum_today",
        "terminal_tx_count_today",
        "terminal_avg_amount_today",

        # Новые Velocity-фичи (Количество транзакций)
        "customer_tx_count_5m",
        "customer_tx_count_1h",
        "customer_tx_count_7d",

        # Новые Velocity-фичи (Суммы транзакций)
        "customer_amount_sum_5m",
        "customer_amount_sum_1h",
        "customer_amount_sum_7d",

        # Контекст среднего чека за месяц
        "customer_avg_amount_30d",
        "customer_current_vs_avg_30d",

        # Циклические фичи времени
        "hour_sin",
        "hour_cos"
    ],
    outputCol="numeric_features",
    # handleInvalid="skip" # Опционально: пропустить строки с null, если они появятся вне fillna
)

scaler = StandardScaler(
    inputCol="numeric_features",
    outputCol="features",
    withStd=True,
    withMean=True,
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="tx_fraud",
)

pipeline = Pipeline(
    stages=[
        numeric_assembler,
        scaler,
        lr,
    ]
)
grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.01,0.1, 0.5])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .build()
    )

evaluator = MulticlassClassificationEvaluator(
    labelCol="tx_fraud",
    predictionCol="prediction",
    metricName="recallByLabel",
    metricLabel=1.0
)

tvs = (
    TrainValidationSplit(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluator,
    parallelism=1, seed=42)
    )

# Разделим данные
logger.info("Starting data separation")
train, test = df_result.randomSplit([0.8, 0.2], seed=42)

logger.info("Starting training/validation process")
tvsModel = tvs.fit(train)
logger.info("Finishing training/validation process")
best_model = tvsModel.bestModel


best_lr = best_model.stages[-1]

print("regParam:", best_lr.getRegParam())
print("elasticNetParam:", best_lr.getElasticNetParam())

for params, metric in zip(grid, tvsModel.validationMetrics):
    print(
        "regParam:",
        params[lr.regParam],
        "elasticNetParam:",
        params[lr.elasticNetParam],
        "recall:",
        metric,
    )



# оценка теста
test_predictions = best_model.transform(test)

test_recall = evaluator.evaluate(test_predictions)

print("Test recall:", test_recall)
