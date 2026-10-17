"""Settings from environment variables (RATELIMIT_CAPACITY, RATELIMIT_BACKEND, ...)
or from a local .env file. Real environment variables win over the file."""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RATELIMIT_", env_file=".env")

    capacity: float = Field(10, gt=0, description="bucket size (burst)")
    refill_rate: float = Field(1, gt=0, description="tokens per second")
    backend: Literal["memory", "redis"] = "memory"
    redis_url: str = "redis://localhost:6379/0"
    test_redis_url: str = Field(
        "redis://localhost:6379/15",
        description="Redis database for tests and scripts/race_demo.py: keep it separate from real data",
    )
