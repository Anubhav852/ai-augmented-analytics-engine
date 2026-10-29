import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "AI_Augmented_Analytics_Engine"
    VERSION: str = "v1.0.0"

    # Infrastructure
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # AI engine (Groq, OpenAI-compatible chat completions API)
    AI_API_URL: str = os.getenv("AI_API_URL", "https://api.groq.com/openai/v1/chat/completions")
    AI_MODEL: str = os.getenv("AI_MODEL", "llama-3.1-8b-instant")
    AI_API_TOKEN: str = os.getenv("AI_API_TOKEN", "")


settings = Settings()