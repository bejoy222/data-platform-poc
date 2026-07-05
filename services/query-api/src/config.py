from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Query API"
    APP_VERSION: str = "0.1.0"

    # Anthropic
    ANTHROPIC_API_KEY: str = ""
    LLM_MODEL: str = "claude-sonnet-4-6"

    # MinIO
    MINIO_ENDPOINT: str = "http://192.168.0.10:9000"
    MINIO_ACCESS_KEY: str = "admin"
    MINIO_SECRET_KEY: str = "changeme123456"

    # Schema Registry
    SCHEMA_REGISTRY_URL: str = "http://192.168.0.10:8000"

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
