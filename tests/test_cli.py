"""Tests for the CLI interface."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from arcana.cli import _parse_bar_spec, cli


class TestCLI:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Arcana" in result.output

    def test_db_init_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "init", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.output

    def test_ingest_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "--since" in result.output
        assert "PAIR" in result.output

    def test_run_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--interval" in result.output

    def test_status_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["status", "--help"])
        assert result.exit_code == 0

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.ingest_backfill")
    def test_ingest_command(self, mock_backfill, mock_source_cls, mock_db_cls):
        mock_backfill.return_value = 42

        # Configure context managers
        mock_source = MagicMock()
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "ETH-USD", "--since", "2025-01-01"])

        assert result.exit_code == 0
        assert "42 new trades ingested" in result.output
        mock_backfill.assert_called_once()
        mock_db.init_schema.assert_called_once()

    def test_ingest_requires_since(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "ETH-USD"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_bars_build_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "--help"])
        assert result.exit_code == 0
        assert "BAR_SPEC" in result.output
        assert "PAIR" in result.output

    @patch("arcana.cli.Database")
    @patch("arcana.cli.build_bars")
    def test_bars_build_command(self, mock_build_bars, mock_db_cls):
        mock_build_bars.return_value = 150

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "tick_500", "ETH-USD"])

        assert result.exit_code == 0
        assert "150 bars built" in result.output
        mock_build_bars.assert_called_once()

        # Verify the builder passed was a TickBarBuilder with correct params
        builder_arg = mock_build_bars.call_args[0][0]
        assert builder_arg.bar_type == "tick_500"

    @patch("arcana.cli.Database")
    @patch("arcana.cli.build_bars")
    def test_bars_build_rebuild_flag(self, mock_build_bars, mock_db_cls):
        """--rebuild should pass rebuild=True to build_bars."""
        mock_build_bars.return_value = 100

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "--rebuild", "tick_500", "ETH-USD"])

        assert result.exit_code == 0
        assert "Rebuilding" in result.output
        assert "100 bars built" in result.output
        # Verify rebuild=True was passed
        _, kwargs = mock_build_bars.call_args
        assert kwargs["rebuild"] is True

    @patch("arcana.cli.Database")
    @patch("arcana.cli.build_bars")
    def test_bars_build_no_rebuild_by_default(self, mock_build_bars, mock_db_cls):
        """Without --rebuild, rebuild should be False."""
        mock_build_bars.return_value = 100

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "tick_500", "ETH-USD"])

        assert result.exit_code == 0
        assert "Building" in result.output
        _, kwargs = mock_build_bars.call_args
        assert kwargs["rebuild"] is False

    def test_bars_build_invalid_spec(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "invalid_spec", "ETH-USD"])
        assert result.exit_code != 0
        assert "Invalid bar spec" in result.output

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.run_daemon")
    def test_run_fails_when_no_data(self, mock_daemon, mock_source_cls, mock_db_cls):
        mock_daemon.side_effect = RuntimeError(
            "No trades found for ETH-USD. Run 'arcana ingest ETH-USD --since <date>' first."
        )

        mock_source = MagicMock()
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["run", "ETH-USD"])

        assert result.exit_code != 0
        assert "No trades found" in result.output


class TestParseBarSpec:
    def test_tick_spec(self):
        builder = _parse_bar_spec("tick_500", "coinbase", "ETH-USD")
        assert builder.bar_type == "tick_500"

    def test_volume_spec(self):
        builder = _parse_bar_spec("volume_100", "coinbase", "ETH-USD")
        assert builder.bar_type == "volume_100"

    def test_dollar_spec(self):
        builder = _parse_bar_spec("dollar_50000", "coinbase", "ETH-USD")
        assert builder.bar_type == "dollar_50000"

    def test_time_minutes(self):
        builder = _parse_bar_spec("time_5m", "coinbase", "ETH-USD")
        assert builder.bar_type == "time_5m"

    def test_time_hours(self):
        builder = _parse_bar_spec("time_1h", "coinbase", "ETH-USD")
        assert builder.bar_type == "time_1h"

    def test_time_seconds(self):
        builder = _parse_bar_spec("time_30s", "coinbase", "ETH-USD")
        assert builder.bar_type == "time_30s"

    def test_time_days(self):
        builder = _parse_bar_spec("time_1d", "coinbase", "ETH-USD")
        assert builder.bar_type == "time_1d"

    def test_decimal_threshold(self):
        builder = _parse_bar_spec("volume_10.5", "coinbase", "ETH-USD")
        assert builder.bar_type == "volume_10.5"

    def test_invalid_spec_raises(self):
        import click

        with pytest.raises(click.exceptions.BadParameter, match="Invalid bar spec"):
            _parse_bar_spec("invalid", "coinbase", "ETH-USD")

    # ── Information-driven bar specs ─────────────────────────────────

    def test_tib_spec(self):
        builder = _parse_bar_spec("tib_20", "coinbase", "ETH-USD")
        assert builder.bar_type == "tib_20"

    def test_vib_spec(self):
        builder = _parse_bar_spec("vib_10", "coinbase", "ETH-USD")
        assert builder.bar_type == "vib_10"

    def test_dib_spec(self):
        builder = _parse_bar_spec("dib_50", "coinbase", "ETH-USD")
        assert builder.bar_type == "dib_50"

    def test_trb_spec(self):
        builder = _parse_bar_spec("trb_10", "coinbase", "ETH-USD")
        assert builder.bar_type == "trb_10"

    def test_vrb_spec(self):
        builder = _parse_bar_spec("vrb_20", "coinbase", "ETH-USD")
        assert builder.bar_type == "vrb_20"

    def test_drb_spec(self):
        builder = _parse_bar_spec("drb_30", "coinbase", "ETH-USD")
        assert builder.bar_type == "drb_30"

    # ── Auto-calibrated dollar bars ──────────────────────────────────

    def test_dollar_auto_spec(self):
        db = MagicMock()
        db.get_dollar_volume_stats.return_value = (10_000_000.0, 10.0)
        builder = _parse_bar_spec("dollar_auto", "coinbase", "ETH-USD", db=db)
        # 10M / (10 days * 50 bars/day) = 20,000
        assert builder.bar_type == "dollar_20000"

    def test_dollar_auto_with_bars_per_day(self):
        db = MagicMock()
        db.get_dollar_volume_stats.return_value = (10_000_000.0, 10.0)
        builder = _parse_bar_spec("dollar_auto_100", "coinbase", "ETH-USD", db=db)
        # 10M / (10 * 100) = 10,000
        assert builder.bar_type == "dollar_10000"

    def test_dollar_auto_requires_db(self):
        import click

        with pytest.raises(click.exceptions.UsageError, match="database connection"):
            _parse_bar_spec("dollar_auto", "coinbase", "ETH-USD")

    @patch("arcana.cli.Database")
    @patch("arcana.cli.build_bars")
    @patch("arcana.cli.calibrate_dollar_threshold")
    def test_bars_build_dollar_auto_command(self, mock_calibrate, mock_build_bars, mock_db_cls):
        mock_calibrate.return_value = 200_000
        mock_build_bars.return_value = 250

        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["bars", "build", "dollar_auto", "ETH-USD"])

        assert result.exit_code == 0
        assert "250 bars built" in result.output
        mock_calibrate.assert_called_once()

    # ── Auto-calibrated tick/volume bars ──────────────────────────────

    def test_tick_auto_spec(self):
        db = MagicMock()
        # 500k trades over 100 days → threshold=100
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        builder = _parse_bar_spec("tick_auto", "coinbase", "ETH-USD", db=db)
        assert builder.bar_type == "tick_100"

    def test_tick_auto_with_bars_per_day(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        builder = _parse_bar_spec("tick_auto_100", "coinbase", "ETH-USD", db=db)
        # 500k / (100 * 100) = 50
        assert builder.bar_type == "tick_50"

    def test_tick_auto_requires_db(self):
        import click

        with pytest.raises(click.exceptions.UsageError, match="database connection"):
            _parse_bar_spec("tick_auto", "coinbase", "ETH-USD")

    def test_volume_auto_spec(self):
        db = MagicMock()
        # 100k volume over 100 days → 20 per bar
        db.get_trade_volume_stats.return_value = (500_000.0, 100_000.0, 100.0)
        builder = _parse_bar_spec("volume_auto", "coinbase", "ETH-USD", db=db)
        assert builder.bar_type == "volume_20"

    def test_volume_auto_requires_db(self):
        import click

        with pytest.raises(click.exceptions.UsageError, match="database connection"):
            _parse_bar_spec("volume_auto", "coinbase", "ETH-USD")

    # ── Info-bar auto-calibration of E₀ ──────────────────────────────

    def test_info_bar_auto_calibrates_e0_when_db_available(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.60)

        builder = _parse_bar_spec("tib_10", "coinbase", "ETH-USD", db=db)
        assert builder.bar_type == "tib_10"
        # E[T]=100, bias=0.2, contrib=1.0 → E0=20.0
        assert builder._ewma.expected == pytest.approx(20.0)

    def test_info_bar_uses_explicit_initial_expected(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.60)

        builder = _parse_bar_spec(
            "tib_10", "coinbase", "ETH-USD", db=db, initial_expected=999.0
        )
        assert builder._ewma.expected == pytest.approx(999.0)

    def test_info_bar_fallback_to_zero_without_db(self):
        builder = _parse_bar_spec("tib_10", "coinbase", "ETH-USD")
        assert builder._ewma.expected == 0.0
