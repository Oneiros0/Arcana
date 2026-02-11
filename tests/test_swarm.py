"""Tests for the parallel backfill swarm module."""

from datetime import datetime, timedelta, timezone

import pytest
import yaml

from arcana.swarm import (
    format_worker_summary,
    generate_compose,
    split_range,
)


class TestSplitRange:
    def test_single_worker(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)
        chunks = split_range(since, until, 1)

        assert len(chunks) == 1
        assert chunks[0] == (since, until)

    def test_two_workers_split_evenly(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 7, 1, tzinfo=timezone.utc)
        chunks = split_range(since, until, 2)

        assert len(chunks) == 2
        assert chunks[0][0] == since
        assert chunks[1][1] == until
        # Adjacent chunks share boundary
        assert chunks[0][1] == chunks[1][0]

    def test_twelve_workers(self):
        since = datetime(2022, 1, 1, tzinfo=timezone.utc)
        until = datetime(2023, 1, 1, tzinfo=timezone.utc)
        chunks = split_range(since, until, 12)

        assert len(chunks) == 12
        assert chunks[0][0] == since
        assert chunks[-1][1] == until

        # No gaps: each chunk end is next chunk start
        for i in range(len(chunks) - 1):
            assert chunks[i][1] == chunks[i + 1][0]

    def test_covers_full_range(self):
        """Union of all chunks covers exactly since→until."""
        since = datetime(2022, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
        until = datetime(2024, 3, 20, 14, 0, 0, tzinfo=timezone.utc)
        chunks = split_range(since, until, 7)

        assert chunks[0][0] == since
        assert chunks[-1][1] == until

    def test_invalid_workers_zero(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="workers must be >= 1"):
            split_range(since, until, 0)

    def test_invalid_since_after_until(self):
        since = datetime(2025, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="must be before"):
            split_range(since, until, 4)

    def test_large_worker_count(self):
        """48 workers across 4 years — no crashes, no gaps."""
        since = datetime(2022, 1, 1, tzinfo=timezone.utc)
        until = datetime(2026, 1, 1, tzinfo=timezone.utc)
        chunks = split_range(since, until, 48)

        assert len(chunks) == 48
        assert chunks[0][0] == since
        assert chunks[-1][1] == until

        for i in range(47):
            assert chunks[i][1] == chunks[i + 1][0]

    def test_chunks_are_roughly_equal(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 1, tzinfo=timezone.utc)
        chunks = split_range(since, until, 4)

        durations = [(end - start).total_seconds() for start, end in chunks]
        # All within 1 second of each other
        assert max(durations) - min(durations) < 1.0


class TestGenerateCompose:
    def test_generates_valid_yaml(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 4, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=3)

        # Should be serializable to YAML
        output = yaml.dump(compose, default_flow_style=False)
        parsed = yaml.safe_load(output)
        assert "services" in parsed

    def test_correct_number_of_services(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 7, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=6)

        # 6 workers + 1 db = 7 services
        assert len(compose["services"]) == 7
        assert "db" in compose["services"]

    def test_worker_commands_have_since_until(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 3, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=2)

        workers = {k: v for k, v in compose["services"].items() if k != "db"}
        assert len(workers) == 2

        for name, svc in workers.items():
            cmd = svc["command"]
            assert "ingest" in cmd
            assert "ETH-USD" in cmd
            assert "--since" in cmd
            assert "--until" in cmd

    def test_workers_have_env_vars(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 2, 1, tzinfo=timezone.utc)
        compose = generate_compose(
            "ETH-USD", since, until, workers=1,
            db_user="myuser", db_password="secret",
        )

        workers = {k: v for k, v in compose["services"].items() if k != "db"}
        worker = list(workers.values())[0]

        assert worker["environment"]["ARCANA_DB_USER"] == "myuser"
        assert worker["environment"]["ARCANA_DB_PASSWORD"] == "secret"

    def test_workers_depend_on_db(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 2, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=2)

        workers = {k: v for k, v in compose["services"].items() if k != "db"}
        for name, svc in workers.items():
            assert "db" in svc["depends_on"]

    def test_db_has_healthcheck(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 2, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=1)

        assert "healthcheck" in compose["services"]["db"]

    def test_workers_have_rate_delay(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 4, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=4)

        workers = {k: v for k, v in compose["services"].items() if k != "db"}
        for name, svc in workers.items():
            # 4 workers * 0.12 = 0.48s per worker
            assert svc["environment"]["ARCANA_RATE_DELAY"] == "0.48"

    def test_restart_on_failure(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 2, 1, tzinfo=timezone.utc)
        compose = generate_compose("ETH-USD", since, until, workers=1)

        workers = {k: v for k, v in compose["services"].items() if k != "db"}
        for name, svc in workers.items():
            assert svc["restart"] == "on-failure"


class TestFormatSummary:
    def test_summary_contains_key_info(self):
        since = datetime(2022, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 1, tzinfo=timezone.utc)
        summary = format_worker_summary("ETH-USD", since, until, 12)

        assert "ETH-USD" in summary
        assert "2022-01-01" in summary
        assert "2024-01-01" in summary
        assert "12" in summary
        assert "730 days" in summary

    def test_summary_shows_rate_info(self):
        since = datetime(2022, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 1, tzinfo=timezone.utc)
        summary = format_worker_summary("ETH-USD", since, until, 4)

        assert "delay" in summary
        assert "req/s" in summary

    def test_summary_lists_all_workers(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 4, 1, tzinfo=timezone.utc)
        summary = format_worker_summary("ETH-USD", since, until, 3)

        lines = summary.strip().split("\n")
        # Header + rate info + column header + divider + 3 workers = at least 7 lines
        assert len(lines) >= 7


class TestSwarmCLI:
    """Tests for swarm CLI commands (help text and basic invocation)."""

    def test_swarm_help(self):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["swarm", "--help"])
        assert result.exit_code == 0
        assert "Parallel backfill" in result.output

    def test_swarm_launch_help(self):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["swarm", "launch", "--help"])
        assert result.exit_code == 0
        assert "--since" in result.output
        assert "--workers" in result.output
        assert "--until" in result.output

    def test_swarm_launch_generates_file(self, tmp_path):
        from click.testing import CliRunner
        from arcana.cli import cli

        output = tmp_path / "test-compose.yml"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "swarm", "launch", "ETH-USD",
            "--since", "2024-01-01",
            "--until", "2024-04-01",
            "--workers", "3",
            "--output", str(output),
        ])

        assert result.exit_code == 0
        assert output.exists()

        # Parse and verify
        with open(output) as f:
            compose = yaml.safe_load(f)
        assert len(compose["services"]) == 4  # 3 workers + db

    def test_swarm_validate_help(self):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["swarm", "validate", "--help"])
        assert result.exit_code == 0
        assert "--gap-threshold" in result.output

    def test_swarm_status_help(self):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["swarm", "status", "--help"])
        assert result.exit_code == 0

    def test_swarm_stop_help(self):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["swarm", "stop", "--help"])
        assert result.exit_code == 0
        assert "--remove-volumes" in result.output

    def test_swarm_stop_missing_file(self, tmp_path):
        from click.testing import CliRunner
        from arcana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "swarm", "stop",
            "--file", str(tmp_path / "nonexistent.yml"),
        ])
        assert result.exit_code == 1
        assert "not found" in result.output
