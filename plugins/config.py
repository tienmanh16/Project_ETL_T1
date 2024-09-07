import os
import dotenv


class AppConfig(object):
    """
    Access environment variables here.
    """
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_SERVICE = os.getenv("DB_SERVICE")

    DB_NAME_1 = os.getenv("DB_NAME_1")
    DB_USER_1 = os.getenv("DB_USER_1")
    DB_SCHEMA_1 = os.getenv("DB_SCHEMA_1")
    DB_PASSWORD_1 = os.getenv("DB_PASSWORD_1")
    
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET")

    OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")

    DB_POSTGRES_NAME = os.getenv("DB_POSTGRES_NAME")
    DB_POSTGRES_USER = os.getenv("DB_POSTGRES_USER")
    DB_POSTGRES_PASSWORD = os.getenv("DB_POSTGRES_PASSWORD")
    DB_POSTGRES_PORT = os.getenv("DB_POSTGRES_PORT")


    DB_STARROCKS_SCHEMA = os.getenv("DB_STARROCKS_SCHEMA")
    DB_STARROCKS_USER = os.getenv("DB_STARROCKS_USER")
    DB_STARROCKS_PASSWORD = os.getenv("DB_STARROCKS_PASSWORD")
    DB_STARROCKS_PORT = os.getenv("DB_STARROCKS_PORT")
config = AppConfig()
