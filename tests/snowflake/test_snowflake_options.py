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

import pytest

from ai.starlake.common import MissingEnvironmentVariable
from ai.starlake.snowflake import StarlakeSnowflakeJob

from tests.snowflake.conftest import _SNOWFLAKE_TEST_MODULE_NAME


# ------------------------------------------------------------------
# 4.1  get_context_var resolution chain for Snowflake
# ------------------------------------------------------------------

class TestStarlakeOptions:
    """Test StarlakeOptions.get_context_var() resolution for Snowflake.

    Snowflake inherits the base StarlakeOptions chain:
    options -> default -> env var.
    """

    def test_options_dict_takes_precedence(self):
        result = StarlakeSnowflakeJob.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={"my_var": "from_options"},
        )
        assert result == "from_options"

    def test_default_value_second(self):
        result = StarlakeSnowflakeJob.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={},
        )
        assert result == "from_default"

    def test_env_var_third(self, monkeypatch):
        monkeypatch.setenv("my_var", "from_env")
        result = StarlakeSnowflakeJob.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_env"

    def test_raises_when_nothing_found(self, monkeypatch):
        monkeypatch.delenv("nonexistent_var", raising=False)
        with pytest.raises(MissingEnvironmentVariable):
            StarlakeSnowflakeJob.get_context_var(
                var_name="nonexistent_var",
                default_value=None,
                options={},
            )


# ------------------------------------------------------------------
# 4.2  StarlakeSnowflakeJob constructor
# ------------------------------------------------------------------

class TestStarlakeSnowflakeJob:

    def test_constructor_default_retries(self, snowflake_job):
        """Default retries is 1."""
        assert snowflake_job.retries == 1

    def test_constructor_default_sl_env_vars(self, snowflake_job):
        """Default job has sl_env_vars dict."""
        assert isinstance(snowflake_job.sl_env_vars, dict)

    def test_constructor_stage_location(self, snowflake_job):
        """Stage location is set from constructor kwargs."""
        assert snowflake_job.stage_location == "staging"

    def test_constructor_warehouse(self, snowflake_job):
        """Warehouse is set from constructor kwargs."""
        assert snowflake_job.warehouse == "COMPUTE_WH"

    def test_constructor_packages(self, snowflake_job):
        """Packages include mandatory packages."""
        packages = snowflake_job.packages
        assert "croniter" in packages
        assert "python-dateutil" in packages
        assert "snowflake-snowpark-python" in packages

    def test_constructor_allow_overlapping_execution(self, snowflake_job):
        """Default allow_overlapping_execution is False."""
        assert snowflake_job.allow_overlapping_execution is False

    def test_constructor_custom_options(self):
        """Custom options are propagated."""
        job = StarlakeSnowflakeJob(
            filename="test_custom.py",
            module_name=_SNOWFLAKE_TEST_MODULE_NAME,
            options={
                "stage_location": "custom_stage",
                "warehouse": "CUSTOM_WH",
                "retries": "3",
            },
        )
        assert job.stage_location == "custom_stage"
        assert job.warehouse == "CUSTOM_WH"
        assert job.retries == 3

    def test_sl_execution_environment_is_sql(self):
        from ai.starlake.job import StarlakeExecutionEnvironment
        assert StarlakeSnowflakeJob.sl_execution_environment() == StarlakeExecutionEnvironment.SQL

    def test_sl_orchestrator_is_snowflake(self):
        from ai.starlake.job import StarlakeOrchestrator
        assert StarlakeSnowflakeJob.sl_orchestrator() == StarlakeOrchestrator.SNOWFLAKE


# ------------------------------------------------------------------
# 4.3  sl_load produces DAGTask with StoredProcedureCall
# ------------------------------------------------------------------

class TestSnowflakeSlLoadTask:

    def test_sl_load_produces_dag_task(self, snowflake_job, snowflake_dag_context):
        """sl_load() returns a DAGTask with StoredProcedureCall definition."""
        from snowflake.core.task import StoredProcedureCall
        from snowflake.core.task.dagv1 import DAGTask

        task = snowflake_job.sl_load(
            task_id="load_starbake_customers",
            domain="starbake",
            table="customers",
        )
        assert isinstance(task, DAGTask)
        assert isinstance(task.definition, StoredProcedureCall)
        assert task.comment == "Starlake load starbake.customers"

    def test_sl_load_comment_format(self, snowflake_job, snowflake_dag_context):
        """sl_load() task comment follows 'Starlake load {domain}.{table}' format."""
        task = snowflake_job.sl_load(
            task_id="load_starbake_orders",
            domain="starbake",
            table="orders",
        )
        assert task.comment == "Starlake load starbake.orders"


# ------------------------------------------------------------------
# 4.4  sl_transform produces DAGTask with StoredProcedureCall
# ------------------------------------------------------------------

class TestSnowflakeSlTransformTask:

    def test_sl_transform_produces_dag_task(self, snowflake_job, snowflake_dag_context):
        """sl_transform() returns a DAGTask with StoredProcedureCall definition."""
        from snowflake.core.task import StoredProcedureCall
        from snowflake.core.task.dagv1 import DAGTask

        task = snowflake_job.sl_transform(
            task_id="kpi_order_summary",
            transform_name="kpi.order_summary",
        )
        assert isinstance(task, DAGTask)
        assert isinstance(task.definition, StoredProcedureCall)
        assert task.comment == "Starlake transform kpi.order_summary"

    def test_sl_transform_comment_format(self, snowflake_job, snowflake_dag_context):
        """sl_transform() task comment follows 'Starlake transform {sink}' format."""
        task = snowflake_job.sl_transform(
            task_id="kpi_top_customers",
            transform_name="kpi.top_customers",
        )
        assert task.comment == "Starlake transform kpi.top_customers"


# ------------------------------------------------------------------
# 4.5  start_op creates root task with condition
# ------------------------------------------------------------------

class TestSnowflakeStartOp:

    def test_start_op_creates_task_with_condition(self, snowflake_job, snowflake_dag_context):
        """start_op() creates a DAGTask with SYSTEM$GET_PREDECESSOR condition."""
        task = snowflake_job.start_op(
            task_id="start_task",
            scheduled=True,
            not_scheduled_datasets=None,
            least_frequent_datasets=None,
            most_frequent_datasets=None,
        )
        assert task is not None


# ------------------------------------------------------------------
# 4.6  dummy_op creates SELECT 1 task
# ------------------------------------------------------------------

class TestSnowflakeDummyOp:

    def test_dummy_op_creates_select_task(self, snowflake_job, snowflake_dag_context):
        """dummy_op() creates a DAGTask with 'select' definition."""
        task = snowflake_job.dummy_op(task_id="fake_task")
        assert task is not None
        assert task.name == "fake_task"
        assert "fake_task" in str(task.definition)
