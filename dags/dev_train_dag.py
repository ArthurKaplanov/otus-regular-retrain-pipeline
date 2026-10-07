import datetime
import uuid

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule # type: ignore
from airflow.providers.yandex.operators.dataproc import ( #  type: ignore
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

DATAPROC_SA_ID    = Variable.get("DP_SA_ID")
SUBNET_ID         = Variable.get("YC_SUBNET_ID")
SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")
BUCKET_NAME       = "hw6-dag-bucker"
YC_ZONE           = Variable.get("YC_ZONE")
SSH_PUBLIC_KEY    = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL =Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")

S3_VENV_ARCHIVE = "s3a://hw6-dag-bucker/envs/retrain_venv.tar.gz"

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_NAME = "fraud_detection_test"

with DAG(
    dag_id="prod_fraud_model_training",
    schedule=None,
    start_date=datetime.datetime(2026, 8, 21),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "spark", "dataproc"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=f"fraud-training-{uuid.uuid4().hex[:8]}",
        cluster_description=(
            "Temporary Spark cluster for daily model training"
        ),
        service_account_id= DATAPROC_SA_ID,
        ssh_public_keys=    SSH_PUBLIC_KEY,
        zone=               YC_ZONE,
        subnet_id=          SUBNET_ID,
        security_group_ids=[
                            SECURITY_GROUP_ID,
        ],
        s3_bucket=          BUCKET_NAME,
        services=[
            "YARN",
            "SPARK",
        ],
        cluster_image_version="2.1",

        # HDFS нам не нужен
        datanode_count=0,

        # masternode
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=50,

        # Spark workers
        computenode_resource_preset="s3-c4-m16",
        computenode_disk_type="network-ssd",
        computenode_disk_size=50,
        computenode_count=5,
        dag=dag,
    )

    train_data = DataprocCreatePysparkJobOperator(
        task_id="retrain_model",
        # Используем переменную S3_BUCKET_NAME, которую вы объявили выше
        main_python_file_uri=f"s3a://{BUCKET_NAME}/scripts/train.py",
        args=[
            # Путь к входным данным (заменили хардкод на переменную, если нужно)
            "--input", f"s3a://data-b1gbov23vlrhokj7jp58/features_dev_sample/",

            # Передаем обязательный URI для MLflow
            "--tracking-uri", MLFLOW_TRACKING_URI,

            # Передаем имя эксперимента
            "--experiment-name", MLFLOW_EXPERIMENT_NAME,

            # Передаем S3 доступы, чтобы скрипт мог работать с Object Storage
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,

            "--auto-register",
        ],
        dag=dag,
        properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.dist.archives': f'{S3_VENV_ARCHIVE}#.venv',
            'spark.yarn.appMasterEnv.PYSPARK_PYTHON': './.venv/bin/python3',
            'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON': './.venv/bin/python3',
        },
        )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    create_cluster >> train_data >> delete_spark_cluster # type: ignore
