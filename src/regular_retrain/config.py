import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

YC_ACCESS_KEY = os.getenv("YC_ACCESS_KEY")
YC_SECRET_KEY = os.getenv("YC_SECRET_KEY")

CONFIG_FILE_PATH = Path(__file__).resolve()
PROJECT_PATH = CONFIG_FILE_PATH.parent.parent.parent

DATA_PATH = Path.joinpath(PROJECT_PATH, 'data')

HADOOP_VERSION="3.5.0"
AWS_SDK_VERSION ="1.12.262"
