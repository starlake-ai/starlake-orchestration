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

"""Story 6.2 (issue #86) — Airflow shell sensor mode for the pre-load task.

With ``pre_load_sensor=true`` the bash job builds a
``StarlakePreloadBashSensor`` (reschedule mode, wall-clock timeout) instead of
the one-shot ``StarlakeBashOperator``; the ``skip_or_start`` composition is
preserved through the ``execute``-returns-``True`` XCom contract.

Runs on BOTH Airflow majors (BashSensor import shimmed by compat.py).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME, AIRFLOW_AVAILABLE

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Requires Apache Airflow",
)

SENSOR_OPTIONS = {
    "pre_load_strategy": "imported",
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "42",
    "pre_load_timeout": "120",
    "pre_load_sensor_soft_fail": "true",
}


def _make_job(options: dict):
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    return StarlakeAirflowBashJob(
        filename="test_airflow_sensor.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options=options,
    )


# ---------------------------------------------------------------------------
# 1. Regression pin — sensor mode off keeps the one-shot BashOperator
# ---------------------------------------------------------------------------

class TestSensorOffRegression:

    def test_sensor_off_builds_bash_operator(self):
        from ai.starlake.airflow.compat import BashOperator, BaseSensorOperator

        job = _make_job({"pre_load_strategy": "imported"})
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, BashOperator)
        assert not isinstance(task, BaseSensorOperator)
        # the one-shot preload keeps the exit-code-swallowing xcom wrapper
        assert "return_code=$?" in task.bash_command


# ---------------------------------------------------------------------------
# 2. Sensor construction contract
# ---------------------------------------------------------------------------

class TestSensorConstruction:

    def test_sensor_task_class_and_parameters(self):
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import (
            StarlakePreloadBashSensor,
        )
        from ai.starlake.airflow.compat import BaseSensorOperator

        job = _make_job(dict(SENSOR_OPTIONS))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})

        assert isinstance(task, StarlakePreloadBashSensor)
        assert isinstance(task, BaseSensorOperator)
        assert task.task_id == "check_starbake_incoming_files"
        assert task.poke_interval == 42
        assert task.timeout == 120
        assert task.soft_fail is True
        assert task.mode == "reschedule"
        assert task.retries == 0

    def test_sensor_command_is_raw_with_cwd_prefix(self):
        job = _make_job(dict(SENSOR_OPTIONS))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})

        cmd = task.bash_command
        # BashSensor has no cwd parameter — the command cd's into sl_root
        # (double-quoted: paths may contain spaces, cf. #51)
        assert cmd.startswith(f'cd "{job.sl_root}" && ')
        assert " preload " in cmd
        assert "--strategy imported" in cmd
        # raw command — the exit code must reach the sensor (no echo wrapper)
        assert "return_code=$?" not in cmd
        assert "bash -c" not in cmd

    def test_sensor_env_passthrough(self):
        job = _make_job(dict(SENSOR_OPTIONS))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        # same env semantics as the BashOperator path
        assert task.env == {**job.sl_os_env_vars, **job.sl_env_vars}

    def test_sensor_defaults_when_only_flag_set(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task.poke_interval == 300
        assert task.timeout == 3600
        assert task.soft_fail is False


# ---------------------------------------------------------------------------
# 3. retries setdefault semantics
# ---------------------------------------------------------------------------

class TestSensorRetriesSetdefault:

    def test_explicit_retries_kwarg_wins(self):
        job = _make_job(dict(SENSOR_OPTIONS))
        task = job.sl_pre_load(
            domain="starbake", tables={"customers"}, retries=2
        )
        assert task.retries == 2

    def test_explicit_retries_option_wins(self):
        """Story 6.1 precedence — an explicitly provided retries option still
        reaches the sensor instead of the sensor-branch default 0."""
        job = _make_job(dict(SENSOR_OPTIONS, retries="2"))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task.retries == 2


# ---------------------------------------------------------------------------
# 4. XCom composition — execute returns True; f_skip_or_start honors it
# ---------------------------------------------------------------------------

class TestSensorXComComposition:

    def _make_poke_sensor(self, monkeypatch):
        """Sensor in poke mode for unit-driving execute (reschedule mode's
        execute needs a metadata-DB lookup; the override contract is
        mode-independent and mode='reschedule' is pinned in construction
        tests)."""
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import (
            StarlakePreloadBashSensor,
        )
        job = _make_job(dict(SENSOR_OPTIONS))
        task = job.sl_pre_load(
            domain="starbake", tables={"customers"}, mode="poke"
        )
        assert isinstance(task, StarlakePreloadBashSensor)
        monkeypatch.setattr(type(task), "poke", lambda self, context: True)
        return task

    def test_execute_returns_true_on_success(self, monkeypatch):
        task = self._make_poke_sensor(monkeypatch)
        assert task.execute(context={}) is True

    def test_f_skip_or_start_proceeds_on_true_and_skips_on_none(self):
        job = _make_job(dict(SENSOR_OPTIONS))
        sensor = job.sl_pre_load(domain="starbake", tables={"customers"})
        short_circuit = job.skip_or_start_op(
            task_id="skip_or_start_loading_starbake", upstream_task=sensor
        )
        f_skip_or_start = short_circuit.python_callable

        ti = MagicMock()
        # sensor succeeded → execute pushed return_value=True → proceed
        ti.xcom_pull.return_value = True
        assert f_skip_or_start(sensor.task_id, ti=ti) is True
        ti.xcom_pull.assert_called_with(
            task_ids=sensor.task_id, key="return_value"
        )
        # sensor timed out (skipped or failed) → no XCom → skip downstream
        ti.xcom_pull.return_value = None
        assert f_skip_or_start(sensor.task_id, ti=ti) is False


# ---------------------------------------------------------------------------
# 5. DAG wiring — pre_load >> skip_or_start >> import
# ---------------------------------------------------------------------------

class TestSensorDagWiring:

    def test_pipeline_chain_preserved_in_sensor_mode(self):
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.orchestration import StarlakeSchedule

        orchestration = AirflowOrchestration(job=_make_job(dict(SENSOR_OPTIONS)))
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        with orchestration:
            pipeline = orchestration.sl_create_pipeline(schedule=schedule)
            with pipeline:
                pre_load = pipeline.sl_pre_load(
                    domain="starbake", tables={"customers"}
                )
                skip_or_start = pipeline.skip_or_start(
                    task_id="skip_or_start_loading_starbake",
                    upstream_task=pre_load,
                )
                sl_import = pipeline.sl_import(
                    task_id="import_starbake",
                    domain="starbake",
                    tables={"customers"},
                )
                pre_load >> skip_or_start
                skip_or_start >> sl_import

        dag = pipeline.dag
        check_task = dag.get_task("check_starbake_incoming_files")
        assert "skip_or_start_loading_starbake" in check_task.downstream_task_ids
        sos_task = dag.get_task("skip_or_start_loading_starbake")
        assert "import_starbake" in sos_task.downstream_task_ids


# ---------------------------------------------------------------------------
# 6. Cloud engines now RESOLVE sensor mode (story 6.5, issue #93) — the
#    story-6.2 rejection is superseded. Full cloud-waiting coverage lives in
#    test_airflow_cloud_preload_waiting.py; here we only pin the inversion.
# ---------------------------------------------------------------------------

class TestCloudEngineWaitingSupersedesRejection:

    def test_rejection_helper_is_gone(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        # story 6.5 removed the shell-only rejection — cloud engines wait now
        assert not hasattr(StarlakeAirflowJob, "_reject_pre_load_sensor_kwargs")

    def test_resolver_honors_sensor_mode_without_raising(self):
        from ai.starlake.airflow import StarlakeAirflowJob

        class _FakeDeferrable:
            def __init__(self, task_id, deferrable=False, **kwargs):
                pass

        kwargs = {
            "pre_load_sensor": True,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
        }
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait is not None
        assert wait.mode == "deferrable"
        assert kwargs == {}  # the four sensor kwargs consumed

    def test_resolver_off_returns_none_and_pops_cleanly(self):
        from ai.starlake.airflow import StarlakeAirflowJob

        kwargs = {
            "pre_load_sensor": False,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
            "pool": "default_pool",
        }
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, None)
        assert wait is None
        assert kwargs == {"pool": "default_pool"}
