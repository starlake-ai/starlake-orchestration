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

import os
from unittest.mock import patch, MagicMock

import pytest

from ai.starlake.airflow import StarlakeAirflowOptions, DEFAULT_DAG_ARGS, DEFAULT_POOL
from ai.starlake.airflow.bash import StarlakeAirflowBashJob
from ai.starlake.common import MissingEnvironmentVariable

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME


# ------------------------------------------------------------------
# 4.1  get_context_var resolution chain
# ------------------------------------------------------------------

class TestStarlakeAirflowOptions:
    """Test StarlakeAirflowOptions.get_context_var() resolution order."""

    def test_options_dict_takes_precedence(self):
        """Options dict value is returned first, before default or env."""
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={"my_var": "from_options"},
        )
        assert result == "from_options"

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_default_value_second(self, mock_variable_cls):
        """Default value is returned when options dict does not contain the var."""
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={},
        )
        assert result == "from_default"
        mock_variable_cls.get.assert_not_called()

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_airflow_variable_third(self, mock_variable_cls):
        """Airflow Variable is checked when options and default are absent."""
        mock_variable_cls.get.return_value = "from_airflow_var"
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_airflow_var"

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_env_var_fourth(self, mock_variable_cls, monkeypatch):
        """Environment variable is checked when Airflow Variable returns None."""
        mock_variable_cls.get.return_value = None
        monkeypatch.setenv("my_var", "from_env")
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_env"

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_raises_when_nothing_found(self, mock_variable_cls, monkeypatch):
        """MissingEnvironmentVariable is raised when no source has the var."""
        mock_variable_cls.get.return_value = None
        monkeypatch.delenv("nonexistent_var", raising=False)
        with pytest.raises(MissingEnvironmentVariable):
            StarlakeAirflowOptions.get_context_var(
                var_name="nonexistent_var",
                default_value=None,
                options={},
            )

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_variable_store_failure_falls_through_to_env(self, mock_variable_cls, monkeypatch):
        """An unavailable Variable store (unmigrated/unreachable metadata DB)
        behaves like an unset variable: the chain continues to the environment
        variable instead of crashing DAG parsing (issue #56)."""
        mock_variable_cls.get.side_effect = Exception("no such column: variable.team_name")
        monkeypatch.setenv("my_var", "from_env")
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_env"

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_variable_store_failure_without_env_raises_missing(self, mock_variable_cls, monkeypatch):
        """A Variable store failure with no other source still surfaces as
        MissingEnvironmentVariable, not as the underlying database error."""
        mock_variable_cls.get.side_effect = Exception("no such table: variable")
        monkeypatch.delenv("nonexistent_var", raising=False)
        with pytest.raises(MissingEnvironmentVariable):
            StarlakeAirflowOptions.get_context_var(
                var_name="nonexistent_var",
                default_value=None,
                options={},
            )

    @patch("ai.starlake.airflow.starlake_airflow_options.Variable")
    def test_variable_fetched_with_a_single_guarded_call(self, mock_variable_cls):
        """The value is returned from one Variable.get(default_var=None) call —
        the old double-call raised KeyError if the variable vanished between
        the existence check and the unguarded second fetch (issue #56)."""
        mock_variable_cls.get.return_value = "from_airflow_var"
        result = StarlakeAirflowOptions.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_airflow_var"
        assert mock_variable_cls.get.call_count == 1
        assert mock_variable_cls.get.call_args.kwargs.get("default_var", "MISSING") is None


# ------------------------------------------------------------------
# 4.2  DEFAULT_DAG_ARGS values
# ------------------------------------------------------------------

class TestDefaultDagArgs:

    def test_depends_on_past_false(self):
        assert DEFAULT_DAG_ARGS["depends_on_past"] is False

    def test_retries_one(self):
        assert DEFAULT_DAG_ARGS["retries"] == 1

    def test_start_date_present(self):
        assert "start_date" in DEFAULT_DAG_ARGS

    def test_email_settings(self):
        assert DEFAULT_DAG_ARGS["email_on_failure"] is False
        assert DEFAULT_DAG_ARGS["email_on_retry"] is False


# ------------------------------------------------------------------
# 4.3  StarlakeAirflowBashJob constructor
# ------------------------------------------------------------------

class TestStarlakeAirflowBashJob:

    def test_constructor_default_pool(self, airflow_job):
        """Default pool is 'default_pool'."""
        assert airflow_job.pool == DEFAULT_POOL

    def test_constructor_custom_options(self):
        """Custom options are passed through to the job."""
        job = StarlakeAirflowBashJob(
            filename="test_custom.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={"default_pool": "custom_pool"},
        )
        assert job.pool == "custom_pool"

    def test_sl_execution_environment_is_shell(self):
        """Shell executor returns SHELL execution environment."""
        from ai.starlake.job import StarlakeExecutionEnvironment
        assert StarlakeAirflowBashJob.sl_execution_environment() == StarlakeExecutionEnvironment.SHELL

    def test_sl_orchestrator_is_airflow(self):
        """StarlakeAirflowBashJob reports AIRFLOW as orchestrator."""
        from ai.starlake.job import StarlakeOrchestrator
        assert StarlakeAirflowBashJob.sl_orchestrator() == StarlakeOrchestrator.AIRFLOW

    def test_sl_job_quotes_options_value(self):
        """The --options value is quoted so env var values containing spaces
        (e.g. a PATH with 'Application Support') survive bash -c word splitting."""
        import json
        import shlex
        from ai.starlake.job import TaskType

        job = StarlakeAirflowBashJob(
            filename="test_options_quoting.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={
                "sl_env_var": json.dumps({
                    "SL_ROOT": "/tmp/project",
                    "PATH": "/opt/with space/bin:/usr/bin",
                })
            },
        )
        operator = job.sl_job(
            task_id="transform_task",
            arguments=["transform", "--name", "kpi.order_summary"],
            task_type=TaskType.TRANSFORM,
        )
        tokens = shlex.split(operator.bash_command)
        options_value = tokens[tokens.index("--options") + 1]
        assert "PATH=/opt/with space/bin:/usr/bin" in options_value

    def test_sl_transform_converts_starlake_dataset_inlets(self):
        """StarlakeDataset inlets are converted to Airflow Datasets (Assets on
        Airflow 3) so Airflow 2's lineage hook can JSON-serialize them to XCom
        in post_execute."""
        from ai.starlake.airflow.compat import Dataset
        from ai.starlake.dataset import StarlakeDataset

        job = StarlakeAirflowBashJob(
            filename="test_inlets.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={},
        )
        operator = job.sl_transform(
            task_id="transform_task",
            transform_name="kpi.order_summary",
            inlets=[StarlakeDataset(name="starbake.orders", cron="0 * * * *")],
        )
        assert operator.inlets
        assert all(isinstance(inlet, Dataset) for inlet in operator.inlets)


# ------------------------------------------------------------------
# 4.4  AirflowDataset.to_event produces Dataset instances
# ------------------------------------------------------------------

class TestAirflowDataset:

    def test_to_event_produces_dataset(self):
        """AirflowDataset.to_event() returns an Airflow Dataset (Asset on 3.x)
        instance."""
        from ai.starlake.airflow.compat import Dataset
        from ai.starlake.airflow import AirflowDataset
        from ai.starlake.dataset import StarlakeDataset

        ds = StarlakeDataset(name="starbake.customers", cron="0 * * * *")
        event = AirflowDataset.to_event(dataset=ds, source="test_source")
        assert isinstance(event, Dataset)
        assert ds.uri in event.uri


# ------------------------------------------------------------------
# 4.5  AirflowPipeline DAG with cron schedule
# ------------------------------------------------------------------

class TestAirflowPipeline:

    def test_dag_with_cron_schedule(self):
        """Pipeline with cron schedule produces valid DAG object."""
        from airflow import DAG
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.orchestration import StarlakeSchedule

        job = StarlakeAirflowBashJob(
            filename="test_cron.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={},
        )
        orch = AirflowOrchestration(job=job)
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        pipeline = orch.sl_create_pipeline(schedule=schedule)
        with pipeline:
            pass
        assert isinstance(pipeline.dag, DAG)
        from ai.starlake.airflow.compat import supports_assets
        if supports_assets():
            # Airflow 3 removed DAG.schedule_interval; the cron is carried
            # by the timetable's expression
            assert pipeline.dag.timetable.expression == "0 0 * * *"
        else:
            assert pipeline.dag.schedule_interval == "0 0 * * *"

    # ------------------------------------------------------------------
    # 4.6  AirflowPipeline DAG with dataset triggers
    # ------------------------------------------------------------------

    def _make_dataset_pipeline(self, strategy):
        """Helper: create a dataset-triggered pipeline with the given strategy."""
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.orchestration import (
            StarlakeDependencies,
            StarlakeDependency,
            StarlakeDependencyType,
        )

        job = StarlakeAirflowBashJob(
            filename="test_dataset.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": strategy},
        )
        orch = AirflowOrchestration(job=job)
        dependencies = StarlakeDependencies([
            StarlakeDependency(
                name="overall_kpis",
                dependency_type=StarlakeDependencyType.TASK,
                dependencies=[
                    StarlakeDependency(
                        name="starbake.orders",
                        dependency_type=StarlakeDependencyType.TABLE,
                        cron="0 * * * *",
                    ),
                    StarlakeDependency(
                        name="starbake.customers",
                        dependency_type=StarlakeDependencyType.TABLE,
                        cron="0 0 * * *",
                    ),
                ],
            ),
        ])
        pipeline = orch.sl_create_pipeline(dependencies=dependencies)
        with pipeline:
            pass
        return pipeline

    def test_dag_with_dataset_triggers_any(self):
        """Pipeline with ANY strategy produces a dataset/asset-triggered DAG
        carrying all upstream datasets as events.

        On Airflow >= 2.9 this uses a ``DatasetAny`` (``|``) condition; below 2.9
        (no conditional operators) it degrades to a native flat-list dataset
        schedule — still a dataset-triggered timetable (issue #125)."""
        from ai.starlake.dataset import DatasetTriggeringStrategy

        pipeline = self._make_dataset_pipeline("any")
        assert pipeline.dag is not None
        # Timetable is data-driven (DatasetTriggeredTimetable on Airflow 2,
        # AssetTriggeredTimetable on Airflow 3)
        timetable_cls = type(pipeline.dag.timetable).__name__
        assert "Dataset" in timetable_cls or "Asset" in timetable_cls, (
            f"Expected dataset/asset timetable, got {timetable_cls}"
        )
        # Events should contain both upstream datasets
        assert len(pipeline.events) == 2
        event_uris = {e.uri for e in pipeline.events}
        assert "starbake_orders" in event_uris
        assert "starbake_customers" in event_uris
        # Strategy propagated
        assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY

    def test_dag_with_dataset_triggers_all(self):
        """Pipeline with ALL strategy also uses a dataset/asset-triggered
        timetable."""
        from ai.starlake.dataset import DatasetTriggeringStrategy

        pipeline = self._make_dataset_pipeline("all")
        assert pipeline.dag is not None
        timetable_cls = type(pipeline.dag.timetable).__name__
        assert "Dataset" in timetable_cls or "Asset" in timetable_cls
        assert len(pipeline.events) == 2
        assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ALL

    def test_any_below_2_9_falls_back_to_all_with_warning(self, caplog):
        """Below Airflow 2.9 (no DatasetAny/``|``) the ANY strategy degrades to
        a native flat-list dataset schedule and logs a warning; on 2.9+ it uses
        the conditional operator natively and does not warn (issue #125)."""
        import logging

        from ai.starlake.airflow.compat import supports_dataset_conditions

        with caplog.at_level(logging.WARNING):
            pipeline = self._make_dataset_pipeline("any")
        timetable_cls = type(pipeline.dag.timetable).__name__
        assert "Dataset" in timetable_cls or "Asset" in timetable_cls
        warned = any("falling back to ALL" in r.getMessage() for r in caplog.records)
        assert warned is (not supports_dataset_conditions())
