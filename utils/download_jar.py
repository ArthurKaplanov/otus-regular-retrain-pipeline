import urllib.request
from pathlib import Path
import os


pyspark_jars_dir = f"{Path(__file__).parent.parent}/.venv/lib/python3.12/site-packages/pyspark/jars/"

# НАСТОЯЩИЕ прямые ссылки на скачивание JAR-файлов
urls = [
    "https://maven.org",
    "https://maven.org"
]

print("Начинаем скачивание правильных JAR-файлов...")
for url in urls:
    filename = url.split("/")[-1]
    destination = os.path.join(pyspark_jars_dir, filename)

    # Удаляем старый поврежденный файл, если он остался
    if os.path.exists(destination):
        os.remove(destination)

    print(f"Скачиваем {filename}...")
    urllib.request.urlretrieve(url, destination)
    print(f"Файл успешно сохранен: {destination}")

print("\n Все JAR-файлы успешно обновлены!")
