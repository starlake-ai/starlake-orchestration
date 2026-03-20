#
# Copyright © 2025 Starlake AI (https://starlake.ai)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import subprocess
from pathlib import Path

import duckdb
import pytest
import yaml


# ---------------------------------------------------------------------------
# Smoke tests: validate that the test infrastructure is functional.
# ---------------------------------------------------------------------------


class TestStarlakeCli:
    """Verify that the Starlake CLI is available and operational."""

    @pytest.mark.smoke
    def test_cli_is_available(self, starlake_cli: str) -> None:
        assert starlake_cli, "Starlake CLI path should not be empty"
        assert Path(starlake_cli).is_file(), f"CLI not found at {starlake_cli}"

    @pytest.mark.smoke
    @pytest.mark.integration
    def test_dag_generate_succeeds(
        self, starlake_cli: str, starlake_env: dict, tmp_path: Path
    ) -> None:
        """Run ``starlake dag-generate`` against the sample project."""
        output_dir = tmp_path / "generated-dags"
        output_dir.mkdir()
        env = dict(starlake_env)
        env["LOAD_DAG_REF"] = "airflow_load_shell"
        env["TRANSFORM_DAG_REF"] = "airflow_transform_shell"
        result = subprocess.run(
            [starlake_cli, "dag-generate", "--outputDir", str(output_dir)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, (
            f"dag-generate failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        generated_files = list(output_dir.glob("*.py"))
        assert len(generated_files) > 0, "No DAG files were generated"


class TestDuckDBConnection:
    """Verify that DuckDB connectivity works."""

    @pytest.mark.smoke
    def test_duckdb_connects(self, tmp_path: Path) -> None:
        db_path = tmp_path / "smoke.duckdb"
        conn = duckdb.connect(str(db_path))
        result = conn.execute("SELECT 1 AS smoke_test").fetchone()
        conn.close()
        assert result == (1,)


class TestSampleProjectMetadata:
    """Validate the sample project structure and metadata files."""

    @pytest.mark.smoke
    def test_application_config_exists(self, sample_project_path: Path) -> None:
        app_config = sample_project_path / "metadata" / "application.sl.yml"
        assert app_config.is_file(), f"application.sl.yml not found at {app_config}"

    @pytest.mark.smoke
    def test_application_config_has_duckdb_connection(
        self, sample_project_path: Path
    ) -> None:
        app_config = sample_project_path / "metadata" / "application.sl.yml"
        with open(app_config) as f:
            config = yaml.safe_load(f)
        connections = config["application"]["connections"]
        assert "duckdb" in connections, "DuckDB connection not defined"
        assert connections["duckdb"]["type"] == "jdbc"

    @pytest.mark.smoke
    def test_load_domain_exists(self, sample_project_path: Path) -> None:
        domain_dir = sample_project_path / "metadata" / "load" / "starbake"
        assert domain_dir.is_dir(), f"starbake domain not found at {domain_dir}"
        expected_files = [
            "_config.sl.yml",
            "customers.sl.yml",
            "orders.sl.yml",
            "products.sl.yml",
        ]
        for filename in expected_files:
            assert (domain_dir / filename).is_file(), f"Missing {filename}"

    @pytest.mark.smoke
    def test_schedule_diversity(self, sample_project_path: Path) -> None:
        """Verify that load tables use different schedules (hourly + daily)."""
        domain_dir = sample_project_path / "metadata" / "load" / "starbake"
        orders_config = yaml.safe_load((domain_dir / "orders.sl.yml").read_text())
        orders_schedule = orders_config["table"]["metadata"].get("schedule")
        assert orders_schedule == "hourly", (
            f"orders should have hourly schedule, got {orders_schedule}"
        )
        # customers and products inherit daily from _config.sl.yml
        domain_config = yaml.safe_load((domain_dir / "_config.sl.yml").read_text())
        domain_schedule = domain_config["load"]["metadata"]["schedule"]
        assert domain_schedule == "daily", (
            f"domain default schedule should be daily, got {domain_schedule}"
        )

    @pytest.mark.smoke
    def test_transforms_exist_with_dependency(
        self, sample_project_path: Path
    ) -> None:
        """Verify at least 2 transforms exist and top_customers depends on order_summary."""
        transform_dir = sample_project_path / "metadata" / "transform" / "kpi"
        assert transform_dir.is_dir(), f"kpi transform dir not found at {transform_dir}"
        assert (transform_dir / "order_summary.sl.yml").is_file()
        assert (transform_dir / "order_summary.sql").is_file()
        assert (transform_dir / "top_customers.sl.yml").is_file()
        assert (transform_dir / "top_customers.sql").is_file()
        # Verify top_customers SQL references kpi.order_summary (dependency)
        top_sql = (transform_dir / "top_customers.sql").read_text()
        assert "kpi.order_summary" in top_sql, (
            "top_customers.sql must reference kpi.order_summary for dependency chain"
        )

    @pytest.mark.smoke
    def test_dag_configs_exist_per_orchestrator(
        self, sample_project_path: Path
    ) -> None:
        dags_dir = sample_project_path / "metadata" / "dags"
        expected = [
            "airflow_load_shell.sl.yml",
            "airflow_transform_shell.sl.yml",
            "dagster_load_shell.sl.yml",
            "dagster_transform_shell.sl.yml",
            "snowflake_load_sql.sl.yml",
            "snowflake_transform_sql.sl.yml",
        ]
        for filename in expected:
            assert (dags_dir / filename).is_file(), f"Missing DAG config: {filename}"

    @pytest.mark.smoke
    def test_test_data_files_exist(self, sample_project_path: Path) -> None:
        datasets_dir = sample_project_path / "datasets" / "starbake"
        assert datasets_dir.is_dir(), f"datasets/starbake not found at {datasets_dir}"
        for table in ["customers", "orders", "products"]:
            csv_files = list(datasets_dir.glob(f"{table}.*.csv"))
            assert len(csv_files) > 0, f"No CSV files found for {table}"


class TestIsolatedProject:
    """Verify that the isolated_project fixture produces a working copy."""

    @pytest.mark.smoke
    def test_isolated_copy_is_independent(self, isolated_project) -> None:
        project_path, env = isolated_project
        assert project_path.is_dir()
        assert (project_path / "metadata" / "application.sl.yml").is_file()
        assert env["SL_ROOT"] == str(project_path)
