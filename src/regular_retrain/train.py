
import numpy as np
from config import (
    AWS_SDK_VERSION,
    PROJECT_PATH,
    DATA_PATH,
    HADOOP_VERSION,
    YC_ACCESS_KEY,
    YC_SECRET_KEY,
)
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    NumericType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


# Спецификация JAR для связки со Spark 4.2.0 (Hadoop 3.5.0)

# schema = StructType(
#     [StructField('transaction_id', IntegerType(), True),
#      StructField('tx_datetime', TimestampType(), True),
#      StructField('customer_id', StringType(), True),
#      StructField('terminal_id', StringType(), True),
#      StructField('tx_amount', DoubleType(), True),
#      StructField('tx_time_seconds', LongType(), True),
#      StructField('tx_time_days', LongType(), True),
#      StructField('tx_fraud', IntegerType(), True),
#      StructField('tx_fraud_scenario', StringType(), True)])

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
    # .config("spark.executor.instances", "2")\
    # .config("spark.executor.cores", "3")\
    # .config("spark.executor.memory", "1g")\
    .getOrCreate()
)

df = spark.read.parquet(f"{DATA_PATH}/dataset.parquet/")

# меняем типы файлов для мини датасета
# df = df.withColumns(
#     {"customer_id": f.col('customer_id').cast(StringType()),
#      "terminal_id": f.col('terminal_id').cast(StringType()),
#      "tx_fraud_scenario": f.col('tx_fraud_scenario').cast(StringType()),
#      "tx_fraud": f.col('tx_fraud').cast('int')
#      })


w_customer_today = (
    Window
    .partitionBy("customer_id", "tx_time_days")
    .orderBy("tx_datetime", "transaction_id")
    .rowsBetween(Window.unboundedPreceding, -1)
)

w_terminal_today = (
    Window
    .partitionBy("terminal_id", "tx_time_days")
    .orderBy("tx_datetime", "transaction_id")
    .rowsBetween(Window.unboundedPreceding, -1)
)


df_result = df.withColumns({
    "customer_amount_sum_today": (
        f.sum("tx_amount").over(w_customer_today)
    ),
    "terminal_amount_sum_today": (
        f.sum("tx_amount").over(w_terminal_today)
    ),
    "customer_tx_count_today": (
        f.count("transaction_id").over(w_customer_today)
    ),
    "terminal_tx_count_today": (
        f.count("transaction_id").over(w_terminal_today)
    ),
    "customer_avg_amount_today": (
        f.avg("tx_amount").over(w_customer_today)
    ),
    "terminal_avg_amount_today": (
        f.avg("tx_amount").over(w_terminal_today)
    ),
    "hour_sin": f.sin(
        2 * np.pi * f.hour("tx_datetime") / 24
    ),
    "hour_cos": f.cos(
        2 * np.pi * f.hour("tx_datetime") / 24
    ),
})

df_result = df_result.fillna({
    "customer_amount_sum_today": 0.0,
    "customer_avg_amount_today": 0.0,
    "terminal_amount_sum_today": 0.0,
    "terminal_avg_amount_today": 0.0,
})

numeric_assembler = VectorAssembler(
    inputCols=[
        "tx_amount",
        "customer_amount_sum_today",
        "customer_tx_count_today",
        "customer_avg_amount_today",
        "terminal_amount_sum_today",
        "terminal_tx_count_today",
        "terminal_avg_amount_today",
        "hour_sin",
        "hour_cos",

    ],
    outputCol="numeric_features",
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

train, test = df_result.randomSplit([0.8, 0.2], seed=42)

# Обучение модели
pipeline_model = pipeline.fit(train) # pipeline_model - это трансфформер

# инференс
predictions = pipeline_model.transform(test)

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="tx_fraud",
    predictionCol="prediction",
    metricName="weightedRecall"
)
predictions.coalesce(1).select(
    "transaction_id",
    "tx_fraud",
    "prediction"
    ).write.mode('overwrite').csv(f"{PROJECT_PATH}/data/processed/predictions.csv")

pipeline_model.write().overwrite().save(f'{PROJECT_PATH}/model/lr_model')

recall = evaluator_acc.evaluate(predictions)

print(recall)

spark.stop()
