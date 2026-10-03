"""Tests for the CLI interface."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from arcana.cli import cli
from arcana.ingestion.models import FutureInstrument


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
        assert "Initialize the database schema" in result.output

    def test_ingest_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "--since" in result.output
        assert "PAIR" in result.output

    def test_summon_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["summon", "--help"])
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
        result = runner.invoke(cli, ["summon", "ETH-USD"])

        assert result.exit_code != 0
        assert "No trades found" in result.output


class TestListFuturesCommand:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-futures", "--help"])
        assert result.exit_code == 0
        assert "--underlying" in result.output

    @patch("arcana.cli.CoinbaseSource")
    def test_lists_contracts(self, mock_source_cls):
        mock_source = MagicMock()
        mock_source.list_futures.return_value = [
            FutureInstrument(
                product_id="BIT-30OCT26-CDE",
                underlying="BTC",
                contract_expiry_type="EXPIRING",
                contract_expiry="2026-10-30T16:00:00Z",
                contract_size="0.01",
                venue="FCM",
                display_name="Bitcoin Futures (Oct 2026)",
            )
        ]
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["list-futures", "--underlying", "BTC"])

        assert result.exit_code == 0
        assert "BIT-30OCT26-CDE" in result.output
        assert "1 contract(s)" in result.output
        # 'BTC' parsed into a single-element tuple for the source call
        _, kwargs = mock_source.list_futures.call_args
        assert kwargs["underlyings"] == ("BTC",)

    @patch("arcana.cli.CoinbaseSource")
    def test_all_keyword_passes_none(self, mock_source_cls):
        mock_source = MagicMock()
        mock_source.list_futures.return_value = []
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["list-futures", "--underlying", "ALL"])

        assert result.exit_code == 0
        assert "No matching futures" in result.output
        _, kwargs = mock_source.list_futures.call_args
        assert kwargs["underlyings"] is None

    @patch("arcana.cli.CoinbaseSource")
    def test_perpetual_none_expiry_renders_dash(self, mock_source_cls):
        mock_source = MagicMock()
        mock_source.list_futures.return_value = [
            FutureInstrument(
                product_id="BTC-PERP-INTX",
                underlying="BTC",
                contract_expiry_type="PERPETUAL",
                contract_expiry=None,
                contract_size=None,
                venue="INTX",
                display_name="BTC Perpetual",
            )
        ]
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["list-futures", "--expiry-type", "PERPETUAL", "--include-disabled"]
        )

        assert result.exit_code == 0
        assert "BTC-PERP-INTX" in result.output


class TestMetricsCommands:
    def test_snapshot_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "--help"])
        assert result.exit_code == 0
        assert "snapshot" in result.output.lower()

    def test_metrics_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["metrics", "--help"])
        assert result.exit_code == 0
        assert "--interval" in result.output

    @patch("arcana.cli.CoinbaseSource")
    def test_snapshot_no_pairs_disabled_errors(self, mock_source_cls):
        """With no ids and derivatives disabled (default), error before touching DB."""
        mock_source = MagicMock()
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        with runner.isolated_filesystem():  # no arcana.toml → defaults (disabled)
            result = runner.invoke(cli, ["snapshot"])

        assert result.exit_code != 0
        assert "enabled" in result.output.lower()
        mock_source.list_futures.assert_not_called()

    def _write_cfg(self, path, body: str) -> str:
        cfg = path / "arcana.toml"
        cfg.write_text(body)
        return str(cfg)

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.snapshot_metrics_once")
    def test_snapshot_autodiscovers_from_config(
        self, mock_snap, mock_source_cls, mock_db_cls, tmp_path
    ):
        mock_snap.return_value = 1
        mock_source = MagicMock()
        mock_source.list_futures.return_value = [
            FutureInstrument(
                product_id="BIT-30OCT26-CDE",
                underlying="BTC",
                contract_expiry_type="EXPIRING",
            )
        ]
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = self._write_cfg(tmp_path, '[derivatives]\nenabled = true\nunderlyings = ["BTC"]\n')
        runner = CliRunner()
        result = runner.invoke(cli, ["--config", cfg, "snapshot"])

        assert result.exit_code == 0
        assert "Auto-discovered 1 contract(s)" in result.output
        mock_source.list_futures.assert_called_once()
        assert mock_snap.call_args[0][2] == ["BIT-30OCT26-CDE"]

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.snapshot_metrics_once")
    def test_snapshot_command(self, mock_snap, mock_source_cls, mock_db_cls):
        mock_snap.return_value = 2

        mock_source = MagicMock()
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "BIT-30OCT26-CDE", "ET-30OCT26-CDE"])

        assert result.exit_code == 0
        assert "2 snapshot(s) stored for 2 contract(s)" in result.output
        mock_db.init_schema.assert_called_once()
        pairs_arg = mock_snap.call_args[0][2]
        assert pairs_arg == ["BIT-30OCT26-CDE", "ET-30OCT26-CDE"]

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.poll_metrics")
    def test_metrics_command_passes_interval(self, mock_poll, mock_source_cls, mock_db_cls):
        mock_poll.return_value = 7

        mock_source = MagicMock()
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["metrics", "BIT-30OCT26-CDE", "--interval", "60"])

        assert result.exit_code == 0
        assert "7 snapshot row(s) stored" in result.output
        assert mock_poll.call_args.kwargs["interval"] == 60

    @patch("arcana.cli.Database")
    @patch("arcana.cli.CoinbaseSource")
    @patch("arcana.cli.poll_metrics")
    def test_metrics_uses_config_interval_and_discovery(
        self, mock_poll, mock_source_cls, mock_db_cls, tmp_path
    ):
        """No ids and no --interval → discover + interval both from config."""
        mock_poll.return_value = 3
        mock_source = MagicMock()
        mock_source.list_futures.return_value = [
            FutureInstrument(
                product_id="ET-30OCT26-CDE",
                underlying="ETH",
                contract_expiry_type="EXPIRING",
            )
        ]
        mock_source_cls.return_value.__enter__ = MagicMock(return_value=mock_source)
        mock_source_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_db = MagicMock()
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = tmp_path / "arcana.toml"
        cfg.write_text(
            '[derivatives]\nenabled = true\nunderlyings = ["ETH"]\nmetrics_interval = 120\n'
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["--config", str(cfg), "metrics"])

        assert result.exit_code == 0
        assert mock_poll.call_args.kwargs["interval"] == 120
        assert mock_poll.call_args[0][2] == ["ET-30OCT26-CDE"]


class TestStatusMetrics:
    @patch("arcana.cli.Database")
    def test_status_shows_metric_snapshots(self, mock_db_cls):
        from datetime import UTC, datetime

        mock_db = MagicMock()
        mock_db.get_trade_count.return_value = 1000
        mock_db.get_last_timestamp.return_value = datetime(2026, 10, 3, tzinfo=UTC)
        mock_db.get_metric_count.return_value = 42
        mock_db.get_last_metric_time.return_value = datetime(2026, 10, 3, 19, tzinfo=UTC)
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["status", "BIT-30OCT26-CDE"])

        assert result.exit_code == 0
        assert "Metric snapshots: 42" in result.output
        assert "Last snapshot:" in result.output

    @patch("arcana.cli.Database")
    def test_status_omits_metrics_when_none(self, mock_db_cls):
        mock_db = MagicMock()
        mock_db.get_trade_count.return_value = 5
        mock_db.get_last_timestamp.return_value = None
        mock_db.get_metric_count.return_value = 0
        mock_db_cls.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_db_cls.return_value.__exit__ = MagicMock(return_value=False)

        runner = CliRunner()
        result = runner.invoke(cli, ["status", "ETH-USD"])

        assert result.exit_code == 0
        assert "Metric snapshots" not in result.output
