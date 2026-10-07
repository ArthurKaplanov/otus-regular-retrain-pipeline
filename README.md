# Regular retrain model
**Description**: регулярноe переобучение модели машинного обучения.
Поэтапное достижение конечно цели - регулярное переобучение модели машинного обучения на базе инфраструктуры Yandex cloud

## step-by-step
1. написать pipeline для обучения модели машинного обучения на pyspark - Y;
2. добавить mlflow workflow: регистрация метрик и модели - Y
3. добавить infra - mlflow (+ s3 and postgres) - Y
4. добавить airflow (сервисный аккаунт для airflow + dataproc )

## todo list
- [X] random split заменить на time-based split
пример кода:

## Скрипты
```bash
s3cmd put --recursive dags/ s3://hw6-dag-bucker/dags/
s3cmd put src/regular_retrain/test_data_proc_cluster.py s3://hw6-dag-bucker/scripts/test_data_proc_cluster.py
s3cmd put src/regular_retrain/train.py s3://hw6-dag-bucker/scripts/train.py
```

### виртуальное окружение
Для упаковки виртуального окружения для data proc кластера следуйте инструкции
https://yandex.cloud/en/docs/data-proc/operations/python-env
