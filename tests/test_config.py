"""Tests for the configuration system."""

import os
from unittest.mock import patch

from arcana.config import ArcanaConfig, DerivativesConfig, PipelineConfig


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.pair == "ETH-USD"
        assert cfg.source == "coinbase"


class TestDerivativesConfig:
    def test_defaults(self):
        cfg = DerivativesConfig()
        assert cfg.enabled is False
        assert cfg.underlyings == ["BTC", "ETH"]
        assert cfg.expiry_types == ["EXPIRING"]
        assert cfg.metrics_interval == 300

    def test_independent_default_lists(self):
        """default_factory lists must not be shared between instances."""
        a = DerivativesConfig()
        b = DerivativesConfig()
        a.underlyings.append("SOL")
        assert b.underlyings == ["BTC", "ETH"]


class TestArcanaConfig:
    def test_defaults(self):
        cfg = ArcanaConfig()
        assert cfg.database.host == "localhost"
        assert cfg.pipeline.pair == "ETH-USD"
        # Derivatives present with safe defaults even when unconfigured
        assert cfg.derivatives.enabled is False

    def test_derivatives_from_toml(self, tmp_path):
        toml_file = tmp_path / "deriv.toml"
        toml_file.write_text(
            '[derivatives]\nenabled = true\nunderlyings = ["BTC"]\nmetrics_interval = 60\n'
        )
        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.derivatives.enabled is True
        assert cfg.derivatives.underlyings == ["BTC"]
        assert cfg.derivatives.metrics_interval == 60
        # Unspecified field keeps its default
        assert cfg.derivatives.expiry_types == ["EXPIRING"]

    def test_absent_derivatives_uses_defaults(self, tmp_path):
        """Existing configs with no [derivatives] block still load."""
        toml_file = tmp_path / "legacy.toml"
        toml_file.write_text('[pipeline]\npair = "BTC-USD"\n')
        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.derivatives.enabled is False
        assert cfg.derivatives.underlyings == ["BTC", "ETH"]

    def test_from_toml(self, tmp_path):
        toml_content = """\
[pipeline]
pair = "BTC-USD"

[database]
host = "db.example.com"
port = 5433
password = "secret"
"""
        toml_file = tmp_path / "test.toml"
        toml_file.write_text(toml_content)

        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.pipeline.pair == "BTC-USD"
        assert cfg.database.host == "db.example.com"
        assert cfg.database.port == 5433
        assert cfg.database.password == "secret"

    def test_from_toml_minimal(self, tmp_path):
        """Minimal TOML with just pipeline should work."""
        toml_file = tmp_path / "min.toml"
        toml_file.write_text('[pipeline]\npair = "BTC-USD"\n')
        cfg = ArcanaConfig.from_toml(toml_file)
        assert cfg.pipeline.pair == "BTC-USD"

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
