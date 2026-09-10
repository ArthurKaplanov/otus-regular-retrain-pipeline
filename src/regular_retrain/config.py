import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

YC_ACCESS_KEY = os.getenv("YC_ACCESS_KEY")
YC_SECRET_KEY = os.getenv("YC_SECRET_KEY")

CONFIG_FILE_PATH = Path(__file__).resolve()
PROJECT_PATH = CONFIG_FILE_PATH.parent.parent.parent

DATA_PATH = str(Path.joinpath(PROJECT_PATH, 'data', 'dataset.parquet'))
DATA_MINI_SAMPLE_PATH = "s3a://data-b1gbov23vlrhokj7jp58/mini_sample_november/"
DATA_SAMPLE_PATH = "s3a://data-b1gbov23vlrhokj7jp58/sample_from_20_to_24_november/"

HADOOP_VERSION="3.5.0"
AWS_SDK_VERSION ="1.12.262"
