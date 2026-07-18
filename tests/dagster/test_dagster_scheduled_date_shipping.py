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

"""Story 6.10 (issue #113) — cloud variants ship --scheduledDate for ALL task types.

The fargate and dataproc op bodies built the scheduledDate-carrying argument
vector but only handed it to the execution vehicle in the TRANSFORM branch —
every non-transform task (load, one-shot preload…) shipped the original
build-time arguments, so the container/Spark CLI fell back to its own
current time on backfills and late runs.

Quoting contract (mirrors Airflow issues #99/#101): there is NO consuming
shell on any cloud path — fargate serializes the arguments as a JSON array
into ECS containerOverrides, dataproc ships spark_job.args directly, and on
cloud_run the value sits INSIDE the double-quoted ``--args`` string, so a
``'…'``-quoted datetime reaches the container argv with literal quotes.  The
three cloud variants therefore ship the datetime UNQUOTED; the shell variant
keeps the quotes (a real shell consumes them).  ``sl_timestamp_format`` is
space-free, so the unquoted form survives the space-separated ``^ ^`` args
fragment and the helper's ``" ".join``.

The dataproc tests require dagster-gcp (skip-guarded — run in the local
provider venv); shell, cloud_run and fargate run in CI.
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
    RUN_CONFIG,
    _execute,
)
from tests.dagster.test_dagster_retry_arguments import patch_fargate

# RUN_CONFIG's logical_datetime rendered through sl_timestamp_format
# ('%Y-%m-%dT%H:%M:%S%z') — derived, not hardcoded, so a config change
# cannot silently desynchronize these assertions; the format is space-free,
# so the value is shell-splitting-proof even unquoted
from datetime import datetime

from ai.starlake.common import sl_timestamp_format

EXPECTED_DT = datetime.fromisoformat(
    RUN_CONFIG["ops"][PRELOAD_TASK_ID]["config"]["logical_datetime"]
).strftime(sl_timestamp_format)


def _make_load_node(job):
    return job.sl_load(
        task_id=PRELOAD_TASK_ID,  # reuse the RUN_CONFIG op name
        domain="starbake",
        table="customers",
        retries=0,
    )


def _scheduled_date_value(arguments):
    """The argument immediately following --scheduledDate (None if absent)."""
    for index, arg in enumerate(arguments[:-1]):
        if arg == "--scheduledDate":
            return arguments[index + 1]
    return None


# ---------------------------------------------------------------------------
# 1. Fargate — the helper ships --scheduledDate on non-transform tasks
# ---------------------------------------------------------------------------

class TestFargateScheduledDateShipping:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        return StarlakeDagsterFargateJob(
            filename="test_dagster_scheduled_date_shipping.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**FARGATE_OPTIONS, **options},
        )

    def test_load_ships_unquoted_scheduled_date(self, monkeypatch):
        seam = patch_fargate(monkeypatch, [0])
        result = _execute(_make_load_node(self._make_job({})))

        assert result.success
        assert len(seam.seen_arguments) == 1
        shipped = seam.seen_arguments[0]
        # the container command must carry the logical date — and carry it
        # UNQUOTED: the helper ships the list as a JSON array (ECS argv),
        # no shell ever consumes quotes on this path
        assert _scheduled_date_value(shipped) == EXPECTED_DT
        # 6.9 invariants preserved: verb first, exactly once
        assert shipped[0] == "load"
        assert shipped.count("load") == 1


# ---------------------------------------------------------------------------
# 2. Dataproc — spark_job.args ships --scheduledDate on non-transform tasks
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)
class TestDataprocScheduledDateShipping:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
        return StarlakeDagsterDataprocJob(
            filename="test_dagster_scheduled_date_shipping.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**DATAPROC_OPTIONS, **options},
        )

    def test_load_ships_unquoted_scheduled_date(self, monkeypatch):
        from tests.dagster.test_dagster_dataproc_terminal_state import _patch_client

        submitted, _ = _patch_client(monkeypatch)
        result = _execute(_make_load_node(self._make_job({})))

        assert result.success
        assert len(submitted) == 1
        shipped = submitted[0]["job"]["spark_job"]["args"]
        # Spark receives the args vector directly — no consuming shell, so
        # the datetime must be unquoted
        assert _scheduled_date_value(shipped) == EXPECTED_DT
        assert shipped[0] == "load"
        assert shipped.count("load") == 1


# ---------------------------------------------------------------------------
# 3. Cloud Run — already shipped for all task types; pin the UNQUOTED form
# ---------------------------------------------------------------------------

class TestCloudRunScheduledDateQuoting:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_scheduled_date_shipping.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**CLOUD_RUN_OPTIONS, **options},
        )

    def test_load_ships_unquoted_scheduled_date(self, monkeypatch):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        result = _execute(_make_load_node(self._make_job({})))

        assert result.success
        assert len(calls) == 1
        # the '…' sits INSIDE the double-quoted --args string: the local
        # shell never consumes it, gcloud would ship literal quotes into the
        # container argv — the fragment must carry the bare datetime and no
        # single quote anywhere
        assert f"--scheduledDate {EXPECTED_DT}" in calls[0]
        fragment = calls[0].split('--args "', 1)[1].split('"', 1)[0]
        assert "'" not in fragment


# ---------------------------------------------------------------------------
# 4. Shell — the divergent side of the contract: quotes are KEPT
# ---------------------------------------------------------------------------

class TestShellScheduledDateQuoting:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        return StarlakeDagsterShellJob(
            filename="test_dagster_scheduled_date_shipping.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def test_load_keeps_quoted_scheduled_date(self, monkeypatch):
        import ai.starlake.dagster.shell.starlake_dagster_shell_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        result = _execute(_make_load_node(self._make_job({})))

        assert result.success
        assert len(calls) == 1
        # bash consumes the quotes before the CLI sees the value — the shell
        # variant must KEEP them (guard against over-applying the cloud fix)
        assert f"--scheduledDate '{EXPECTED_DT}'" in calls[0]
