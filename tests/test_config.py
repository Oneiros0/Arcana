"""Tests for the configuration system."""

import os
from unittest.mock import patch

from arcana.config import ArcanaConfig, BarSpecConfig, PipelineConfig


class TestBarSpecConfig:
    def test_defaults(self):
        cfg = BarSpecConfig(spec="tick_500")
        assert cfg.spec == "tick_500"
        assert cfg.enabled is True
        assert cfg.bars_per_day is None
        assert cfg.initial_expected is None

    def test_override_all_fields(self):
        cfg = BarSpecConfig(
            spec="tib_10",
            enabled=False,
            bars_per_day=100,
            initial_expected=42.5,
        )
        assert cfg.spec == "tib_10"
        assert cfg.enabled is False
        assert cfg.bars_per_day == 100
        assert cfg.initial_expected == 42.5


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.pair == "ETH-USD"
        assert cfg.source == "coinbase"
        assert cfg.bars_per_day == 50


class TestArcanaConfig:
    def test_defaults(self):
        cfg = ArcanaConfig()
        assert cfg.database.host == "localhost"
        assert cfg.pipeline.pair == "ETH-USD"
        assert cfg.bars == []

    def test_from_toml(self, tmp_path):
        toml_content = """\
[pipeline]
pair = "BTC-USD"
bars_per_day = 100

[database]
host = "db.example.com"
port = 5433
password = "secret"

[[bars]]
spec = "tick_auto"

[[bars]]
spec = "tib_20"
initial_expected = 500.0
bars_per_day = 75
"""
        toml_file = tmp_path / "test.toml"
        toml_file.write_text(toml_content)

        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.pipeline.pair == "BTC-USD"
        assert cfg.pipeline.bars_per_day == 100
        assert cfg.database.host == "db.example.com"
        assert cfg.database.port == 5433
        assert cfg.database.password == "secret"
        assert len(cfg.bars) == 2
        assert cfg.bars[0].spec == "tick_auto"
        assert cfg.bars[0].enabled is True
        assert cfg.bars[1].spec == "tib_20"
        assert cfg.bars[1].initial_expected == 500.0
        assert cfg.bars[1].bars_per_day == 75

    def test_from_toml_minimal(self, tmp_path):
        """Minimal TOML with only bars should work (everything else defaults)."""
        toml_file = tmp_path / "min.toml"
        toml_file.write_text('[[bars]]\nspec = "tick_500"\n')
        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.pipeline.pair == "ETH-USD"
        assert len(cfg.bars) == 1

    def test_from_toml_disabled_bar(self, tmp_path):
        toml_file = tmp_path / "dis.toml"
        toml_file.write_text('[[bars]]\nspec = "trb_10"\nenabled = false\n')
        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.bars[0].enabled is False

    def test_find_and_load_explicit_path(self, tmp_path):
        toml_file = tmp_path / "explicit.toml"
        toml_file.write_text('[pipeline]\npair = "SOL-USD"\n')
        cfg = ArcanaConfig.find_and_load(str(toml_file))
        assert cfg is not None
        assert cfg.pipeline.pair == "SOL-USD"

    def test_find_and_load_env_var(self, tmp_path):
        toml_file = tmp_path / "env.toml"
        toml_file.write_text('[pipeline]\npair = "AVAX-USD"\n')
        with patch.dict(os.environ, {"ARCANA_CONFIG": str(toml_file)}):
            cfg = ArcanaConfig.find_and_load()
        assert cfg is not None
        assert cfg.pipeline.pair == "AVAX-USD"

    def test_find_and_load_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ARCANA_CONFIG", raising=False)
        cfg = ArcanaConfig.find_and_load()
        assert cfg is None

    def test_find_and_load_cwd_arcana_toml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ARCANA_CONFIG", raising=False)
        toml_file = tmp_path / "arcana.toml"
        toml_file.write_text('[pipeline]\npair = "LINK-USD"\n')
        cfg = ArcanaConfig.find_and_load()
        assert cfg is not None
        assert cfg.pipeline.pair == "LINK-USD"
