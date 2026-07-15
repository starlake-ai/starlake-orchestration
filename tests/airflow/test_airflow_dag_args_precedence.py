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

"""Story 6.1 (issue #87) — default_dag_args precedence.

The ``retries`` / ``retry_delay`` dag-generation options (and the
``default_dag_args`` JSON option) must reach every task of the generated DAG:

1. ``StarlakeAirflowJob.default_dag_args()`` must not mutate the shared
   ``DEFAULT_DAG_ARGS`` module constant (AC3).
2. At pipeline level, the options-derived args win over the caller-module
   ``default_dag_args`` snapshot (AC1) while snapshot-only keys survive (AC4).
3. The orchestrator include computes the module snapshot with the user's
   JSON option winning over the framework constants (AC2).
"""

from __future__ import annotations

import copy
import json
import sys
from datetime import timedelta

import pytest

from ai.starlake.airflow import DEFAULT_DAG_ARGS
from ai.starlake.airflow.bash import StarlakeAirflowBashJob

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME


def _make_job(options: dict) -> StarlakeAirflowBashJob:
    return StarlakeAirflowBashJob(
        filename="test_dag_args_precedence.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options=options,
    )


def _inject_caller_snapshot(monkeypatch, snapshot: dict) -> None:
    """Set a module-level ``default_dag_args`` snapshot on the fake caller
    module, the way template-generated DAG modules do.  monkeypatch restores
    (removes) the attribute on teardown so sibling tests are unaffected."""
    caller = sys.modules[_AIRFLOW_TEST_MODULE_NAME]
    monkeypatch.setattr(caller, "default_dag_args", snapshot, raising=False)


# ------------------------------------------------------------------
# AC3 — default_dag_args() must not mutate DEFAULT_DAG_ARGS
# ------------------------------------------------------------------

class TestDefaultDagArgsNoSharedMutation:

    def test_default_dag_args_leaves_module_constant_unchanged(self):
        """Calling default_dag_args() with retries/retry_delay options returns
        the options-derived values WITHOUT polluting the shared constant —
        the Airflow scheduler parses many DAG modules in one interpreter, so
        a mutated constant bleeds across DAGs."""
        before = copy.deepcopy(DEFAULT_DAG_ARGS)

        job = _make_job({"retries": "0", "retry_delay": "10"})
        dag_args = job.default_dag_args()

        assert dag_args["retries"] == 0
        assert dag_args["retry_delay"] == timedelta(seconds=10)
        # the shared constant is untouched — exact values, not just keys
        assert DEFAULT_DAG_ARGS == before
        assert DEFAULT_DAG_ARGS["retries"] == 1
        assert DEFAULT_DAG_ARGS["retry_delay"] == timedelta(minutes=5)

    def test_default_dag_args_json_option_does_not_leak_into_constant(self):
        """The default_dag_args JSON option is folded into the returned dict
        only — a key absent from DEFAULT_DAG_ARGS must not appear in the
        constant after the call."""
        before = copy.deepcopy(DEFAULT_DAG_ARGS)

        job = _make_job({"default_dag_args": json.dumps({"owner": "data-team"})})
        dag_args = job.default_dag_args()

        assert dag_args["owner"] == "data-team"
        assert "owner" not in DEFAULT_DAG_ARGS
        assert DEFAULT_DAG_ARGS == before


# ------------------------------------------------------------------
# Job-level precedence ladder inside default_dag_args()
# ------------------------------------------------------------------

class TestJobLevelPrecedenceLadder:

    def test_explicit_options_win_over_json_option(self):
        """Explicitly provided retries/retry_delay options rank above the
        default_dag_args JSON option."""
        job = _make_job({
            "default_dag_args": json.dumps({"retries": 5, "retry_delay": 60}),
            "retries": "0",
            "retry_delay": "10",
        })
        dag_args = job.default_dag_args()

        assert dag_args["retries"] == 0
        assert dag_args["retry_delay"] == timedelta(seconds=10)

    def test_json_option_wins_over_core_fallbacks_when_options_absent(self):
        """Without explicit retries/retry_delay options, the JSON option value
        stands — the core fallbacks (retries=1, retry_delay=300) must not
        clobber it (the exact '{"retries": 0}' repro from issue #87)."""
        job = _make_job({"default_dag_args": json.dumps({"retries": 0})})
        dag_args = job.default_dag_args()

        assert dag_args["retries"] == 0
        # untouched framework constant fills the non-overlapping key
        assert dag_args["retry_delay"] == timedelta(minutes=5)


# ------------------------------------------------------------------
# AC1 / AC4 — pipeline-level precedence
# ------------------------------------------------------------------

class TestPipelineDagArgsPrecedence:

    def _create_pipeline(self, options: dict):
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.orchestration import StarlakeSchedule

        orch = AirflowOrchestration(job=_make_job(options))
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        return orch.sl_create_pipeline(schedule=schedule)

    def test_options_win_over_legacy_caller_snapshot(self, monkeypatch):
        """AC1 — a caller module carrying the legacy snapshot
        ``dict(__dag_args, **DEFAULT_DAG_ARGS)`` (retries=1, retry_delay=5min)
        no longer overrides the options-derived args: retries=0/retry_delay=10
        from the job options land in dag.default_args AND on the tasks."""
        legacy_snapshot = dict({}, **DEFAULT_DAG_ARGS)  # constants win — old include line
        assert legacy_snapshot["retries"] == 1  # guard: snapshot really carries the default
        _inject_caller_snapshot(monkeypatch, legacy_snapshot)

        pipeline = self._create_pipeline({"retries": "0", "retry_delay": "10"})

        assert pipeline.dag.default_args["retries"] == 0
        assert pipeline.dag.default_args["retry_delay"] == timedelta(seconds=10)

        # task-level proof: default_args apply at operator construction on
        # both Airflow majors
        with pipeline:
            load_task = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
        assert load_task is not None
        assert load_task.task.retries == 0
        assert load_task.task.retry_delay == timedelta(seconds=10)

    def test_default_dag_args_json_option_reaches_dag(self, monkeypatch):
        """AC1 — an explicit ``default_dag_args`` JSON option wins over the
        legacy caller snapshot too."""
        _inject_caller_snapshot(monkeypatch, dict({}, **DEFAULT_DAG_ARGS))

        pipeline = self._create_pipeline(
            {"default_dag_args": json.dumps({"retries": 0})}
        )

        assert pipeline.dag.default_args["retries"] == 0

    def test_snapshot_only_key_survives(self, monkeypatch):
        """AC4 — a caller-snapshot key that default_dag_args() does not emit
        (e.g. 'owner') still reaches dag.default_args after the flip."""
        _inject_caller_snapshot(monkeypatch, {"owner": "data-team"})

        pipeline = self._create_pipeline({"retries": "0"})

        assert pipeline.dag.default_args["owner"] == "data-team"
        assert pipeline.dag.default_args["retries"] == 0


# ------------------------------------------------------------------
# AC2 — include-level snapshot semantics
# ------------------------------------------------------------------

class TestIncludeSnapshotSemantics:

    def test_user_json_option_wins_over_framework_constants(self):
        """AC2 — the new include expression ``dict(DEFAULT_DAG_ARGS,
        **__dag_args)`` lets the user's default_dag_args JSON option win over
        the framework constants in the module snapshot."""
        __dag_args = json.loads('{"retries": 0}')
        snapshot = dict(DEFAULT_DAG_ARGS, **__dag_args)

        assert snapshot["retries"] == 0
        # non-overlapping framework keys are preserved
        assert snapshot["retry_delay"] == timedelta(minutes=5)
        assert snapshot["depends_on_past"] is False
        # building the snapshot must not mutate the constant
        assert DEFAULT_DAG_ARGS["retries"] == 1
