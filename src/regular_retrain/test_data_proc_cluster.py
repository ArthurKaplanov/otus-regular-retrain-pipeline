"""
Скрипт для тестирпования data proc кластера, скрипт выводит версии библиотек.
Важное уточнение: saveAsTextFile rdd не перезаписывает файл, нужно контролировать, его отсуствия
"""
import sys
import io
from datetime import datetime

def check_libraries():
    output = io.StringIO()
    libs = ['mlflow', 'numpy', 'loguru', 'pyspark']

    output.write(f"=== Проверка окружения (Python 3.8) ===\n")
    output.write(f"Интерпретатор: {sys.executable}\n\n")

    for lib in libs:
        try:
            module = __import__(lib)
            version = getattr(module, '__version__', 'Указана')
            output.write(f"{lib}: {version}\n")
        except ImportError:
            output.write(f"{lib}: НЕ УСТАНОВЛЕНА ❌\n")

    # Записываем результат текстом прямо в S3
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("TestDeps").getOrCreate()

    # Сохраняем лог файл в ваш бакет
    sc = spark.sparkContext
    log_rdd = sc.parallelize([output.getvalue()])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3a://hw6-dag-bucker/reports/deps_report_2_{timestamp}.txt"

    # Сохраняем лог в уникальную папку
    log_rdd.coalesce(1).saveAsTextFile(output_path)
    print(f"Отчет успешно сохранен в: {output_path}")

if __name__ == "__main__":
    check_libraries()
