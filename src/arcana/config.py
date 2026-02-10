"""Configuration for Arcana."""

from pydantic import BaseModel, Field


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


class ArcanaConfig(BaseModel):
    """Top-level configuration."""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    default_pair: str = "ETH-USD"
    default_source: str = "coinbase"
