"""Configuration for Arcana."""

from __future__ import annotations

import logging
import os
import tomllib
from pathlib import Path

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """TimescaleDB connection settings."""

    host: str = "localhost"
    port: int = 5432
    database: str = "arcana"
    user: str = "arcana"
    password: str = ""

    @property
    def dsn(self) -> str:
        """PostgreSQL connection string."""
        pw = f":{self.password}" if self.password else ""
        return f"postgresql://{self.user}{pw}@{self.host}:{self.port}/{self.database}"


class BarSpecConfig(BaseModel):
    """Configuration for a single bar type."""

    spec: str
    enabled: bool = True
    bars_per_day: int | None = None
    initial_expected: float | None = None


class PipelineConfig(BaseModel):
    """Pipeline-wide settings."""

    pair: str = "ETH-USD"
    source: str = "coinbase"
    bars_per_day: int = 50


class ArcanaConfig(BaseModel):
    """Top-level configuration."""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    bars: list[BarSpecConfig] = Field(default_factory=list)

    @classmethod
    def from_toml(cls, path: Path | str) -> ArcanaConfig:
        """Load configuration from a TOML file."""
        path = Path(path)
        with open(path, "rb") as f:
            data = tomllib.load(f)
        return cls.model_validate(data)

    @classmethod
    def find_and_load(cls, explicit_path: str | None = None) -> ArcanaConfig | None:
        """Find and load config: explicit path > ARCANA_CONFIG env > arcana.toml in cwd.

        Returns None if no config file is found.
        """
        if explicit_path:
            logger.info("Loading config from %s", explicit_path)
            return cls.from_toml(explicit_path)
        env_path = os.environ.get("ARCANA_CONFIG")
        if env_path:
            logger.info("Loading config from ARCANA_CONFIG=%s", env_path)
            return cls.from_toml(env_path)
        default = Path("arcana.toml")
        if default.exists():
            logger.info("Loading config from %s", default)
            return cls.from_toml(default)
        return None
