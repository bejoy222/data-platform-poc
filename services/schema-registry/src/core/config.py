from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Schema Registry"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = "postgresql+psycopg2://platform:platform123@localhost:5432/schema_registry"

    # MinIO / S3
    MINIO_ENDPOINT: str = "http://192.168.0.10:9000"
    MINIO_ACCESS_KEY: str = "admin"
    MINIO_SECRET_KEY: str = "changeme123456"

    # Storage paths
    BRONZE_BUCKET: str = "bronze"
    SILVER_BUCKET: str = "silver"
    GOLD_BUCKET: str = "gold"

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
