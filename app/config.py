import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "AI_Augmented_Analytics_Engine"
    VERSION: str = "v1.0.0"
    
    # Infrastructure Tunnels (Points to local Redis for development)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # AI Engine Routing (Points to an open-source inference layer)
    AI_API_URL: str = os.getenv("AI_API_URL", "https://api-inference.huggingface.co/models/facebook/bart-large-cnn")
    AI_API_TOKEN: str = os.getenv("AI_API_TOKEN", "mock_token_for_validation")

settings = Settings()