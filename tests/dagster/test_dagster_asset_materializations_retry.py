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

"""Story 6.10 (issue #115) — op bodies must not mutate the closure assets list.

Same bug class as issue #111, second instance: every variant's op body did
``assets.append(get_asset(…))`` on the list captured at ``sl_job`` build
time.  A RetryPolicy re-execution appended AGAIN, so the succeeding attempt
yielded duplicate AssetMaterialization events (one per prior attempt); the
list is also the CALLER's kwargs list, shared across graph rebuilds.
Reachable since 6.8/6.9 made retries command- and id-safe.

The fix builds a per-attempt copy (``attempt_assets = list(assets)``) in all
four variants.  Tests compare the retried-run event count against a clean
run's count instead of hardcoding (an sl_load yields both the assets-loop
event and the dataset materialization).

The dataproc tests require dagster-gcp (skip-guarded — run in the local
provider venv); shell, cloud_run and fargate run in CI.  Retry tests use
``retry_delay: "1"`` — Dagster's retry delay sleeps for real (~1s).
"""

from __future__ import annotations

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME
from tests.dagster.test_dagster_sl_pre_load_cloud_sensor import (
    CLOUD_RUN_OPTIONS,
    DAGSTER_GCP_AVAILABLE,
    DATAPROC_OPTIONS,
    FARGATE_OPTIONS,
    PRELOAD_TASK_ID,
    _execute,
)
from tests.dagster.test_dagster_retry_arguments import RETRY_OPTIONS, patch_fargate


def _make_load_node(job, retries):
    return job.sl_load(
        task_id=PRELOAD_TASK_ID,  # reuse the RUN_CONFIG op name
        domain="starbake",
        table="customers",
        retries=retries,
    )


def _materialization_count(result):
    from dagster import DagsterEventType
    return len(
        [
            event
            for event in result.all_events
            if event.event_type == DagsterEventType.ASSET_MATERIALIZATION
        ]
    )


def _patch_shell_command(monkeypatch, mod, return_codes):
    calls = []

    def fake_execute(shell_command, **kwargs):
        calls.append(shell_command)
        code = return_codes[min(len(calls), len(return_codes)) - 1]
        return ("out", code)

    monkeypatch.setattr(mod, "execute_shell_command", fake_execute)
    return calls


class _AssetRetryContract:
    """Shared scenario: a retried run must emit exactly as many
    AssetMaterialization events as a clean run, and re-executing the SAME
    node must not accumulate events across runs (the closure list is shared
    with the caller across graph rebuilds)."""

    def _clean_count(self, monkeypatch):
        self._patch(monkeypatch, [0])
        result = _execute(_make_load_node(self._make_job({}), retries=0))
        assert result.success
        count = _materialization_count(result)
        assert count > 0
        return count

    def test_retried_run_emits_no_duplicate_materializations(self, monkeypatch):
        baseline = self._clean_count(monkeypatch)

        self._patch(monkeypatch, [1, 0])
        result = _execute(
            _make_load_node(self._make_job(dict(RETRY_OPTIONS)), retries=1),
            raise_on_error=False,
        )
        assert result.success
        # RED pre-fix: the failed first attempt left its asset in the
        # closure list, so the succeeding attempt yielded one extra event
        assert _materialization_count(result) == baseline

    def test_reexecuting_the_same_node_does_not_accumulate(self, monkeypatch):
        self._patch(monkeypatch, [0])
        node = _make_load_node(self._make_job({}), retries=0)
        first = _execute(node)
        second = _execute(node)
        assert first.success and second.success
        assert _materialization_count(second) == _materialization_count(first)


# ---------------------------------------------------------------------------
# 1. Shell
# ---------------------------------------------------------------------------

class TestShellAssetRetry(_AssetRetryContract):

    def _make_job(self, options: dict):
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        return StarlakeDagsterShellJob(
            filename="test_dagster_asset_materializations_retry.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def _patch(self, monkeypatch, return_codes):
        import ai.starlake.dagster.shell.starlake_dagster_shell_job as mod
        _patch_shell_command(monkeypatch, mod, return_codes)


# ---------------------------------------------------------------------------
# 2. Cloud Run
# ---------------------------------------------------------------------------

class TestCloudRunAssetRetry(_AssetRetryContract):

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_asset_materializations_retry.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**CLOUD_RUN_OPTIONS, **options},
        )

    def _patch(self, monkeypatch, return_codes):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod
        _patch_shell_command(monkeypatch, mod, return_codes)


# ---------------------------------------------------------------------------
# 3. Fargate
# ---------------------------------------------------------------------------

class TestFargateAssetRetry(_AssetRetryContract):

    def _make_job(self, options: dict):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        return StarlakeDagsterFargateJob(
            filename="test_dagster_asset_materializations_retry.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**FARGATE_OPTIONS, **options},
        )

    def _patch(self, monkeypatch, return_codes):
        patch_fargate(monkeypatch, return_codes)


# ---------------------------------------------------------------------------
# 4. Dataproc
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)
class TestDataprocAssetRetry(_AssetRetryContract):

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
        return StarlakeDagsterDataprocJob(
            filename="test_dagster_asset_materializations_retry.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**DATAPROC_OPTIONS, **options},
        )

    def _patch(self, monkeypatch, return_codes):
        from dagster_gcp.dataproc.types import DataprocError

        from tests.dagster.test_dagster_dataproc_terminal_state import _patch_client

        # map exit-code semantics onto the dataproc wait seam: non-zero →
        # the wait raises (failed attempt), zero → terminal DONE
        wait_plan = [
            DataprocError("Job error: ERROR") if code else None
            for code in return_codes
        ]
        _patch_client(monkeypatch, wait_plan=wait_plan)
