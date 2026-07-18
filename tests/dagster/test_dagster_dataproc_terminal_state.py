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

"""Story 6.8 (issue #109) — Dataproc sensor-off path terminal state.

The sensor-off path used to interpret the SUBMISSION response as the job's
terminal state (always ``PENDING`` → every real run took the failure branch)
and reused the definition-time job id on RetryPolicy retries (rejected by
Dataproc — ids are unique per project).  It now mirrors the 6.7 poke shape:
a fresh ``job_id`` per ATTEMPT at execute time, then ``submit_job`` →
``wait_for_job(wait_timeout=dataproc_job_wait_timeout)`` → ``get_job``, with
exceptions routed into the existing failure branch (retry_policy / failure
output / skip_or_start semantics preserved).

Requires dagster-gcp (skip-guarded, like the 6.7 dataproc tests).
"""

from __future__ import annotations

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME
from tests.dagster.test_dagster_sl_pre_load_cloud_sensor import (
    DAGSTER_GCP_AVAILABLE,
    DATAPROC_OPTIONS,
    PRELOAD_TASK_ID,
    _execute,
    _execute_dry_run,
    _execute_with_downstream,
    _make_preload_node,
    _skipped_steps,
    _step_failures,
)

pytestmark = pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)


def _make_job(options: dict):
    from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
    return StarlakeDagsterDataprocJob(
        filename="test_dagster_dataproc_terminal_state.py",
        module_name=_DAGSTER_TEST_MODULE_NAME,
        options={**DATAPROC_OPTIONS, **options},
    )


def _patch_client(monkeypatch, wait_error=None, wait_plan=None, get_state="DONE"):
    """Off-path seam: submit_job returns a NON-terminal submission response
    (PENDING, as the real dagster-gcp client does); the terminal state comes
    from wait_for_job + get_job.  ``wait_error`` makes every wait raise;
    ``wait_plan`` is a per-attempt list (an Exception entry raises, None
    passes); ``get_state`` is the terminal state get_job reports."""
    import copy

    from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob

    submitted = []
    waited = []

    class FakeClient:
        def submit_job(self, job_details):
            submitted.append(copy.deepcopy(job_details))
            return {"status": {"state": "PENDING"}}

        def wait_for_job(self, job_id, wait_timeout=None):
            waited.append((job_id, wait_timeout))
            if wait_plan is not None:
                planned = wait_plan[min(len(waited), len(wait_plan)) - 1]
                if planned is not None:
                    raise planned
            elif wait_error is not None:
                raise wait_error

        def get_job(self, job_id):
            return {"status": {"state": get_state}}

    monkeypatch.setattr(
        StarlakeDagsterDataprocJob, "__client__", lambda self: FakeClient()
    )
    return submitted, waited


def _job_ids(submitted):
    return [details["job"]["reference"]["job_id"] for details in submitted]


class TestOffPathTerminalState:

    def test_waits_for_terminal_state_and_succeeds(self, monkeypatch):
        submitted, waited = _patch_client(monkeypatch)
        node = _make_preload_node(_make_job({"pre_load_strategy": "imported"}))
        result = _execute(node)

        assert result.success
        assert len(submitted) == 1
        job_id = _job_ids(submitted)[0]
        assert job_id.startswith(f"{PRELOAD_TASK_ID}_")
        # the submission response is PENDING, never DONE — the op must poll
        # the job to its terminal state, with the configurable wait budget
        assert waited == [(job_id, 3600)]
        assert result.output_for_node(PRELOAD_TASK_ID, "result") == job_id

    def test_fresh_job_id_per_execution(self, monkeypatch):
        # the SAME node executed twice must submit two DISTINCT ids — the
        # definition-time id would repeat (and a RetryPolicy retry would be
        # rejected by Dataproc)
        node = _make_preload_node(_make_job({"pre_load_strategy": "imported"}))
        submitted1, _ = _patch_client(monkeypatch)
        assert _execute(node).success
        first = _job_ids(submitted1)

        submitted2, _ = _patch_client(monkeypatch)
        assert _execute(node).success
        second = _job_ids(submitted2)

        assert len(first) == len(second) == 1
        assert first[0] != second[0]

    def test_wait_error_routes_to_skip_for_preload(self, monkeypatch):
        from dagster_gcp.dataproc.types import DataprocError

        submitted, waited = _patch_client(
            monkeypatch, wait_error=DataprocError("Job error: ERROR")
        )
        node = _make_preload_node(_make_job({"pre_load_strategy": "imported"}))
        result, downstream_calls = _execute_with_downstream(node)

        # preload forces skip_or_start → a failed job skips the downstream
        # loads instead of crashing the run
        assert result.success
        assert len(submitted) == 1
        assert len(waited) == 1
        assert downstream_calls == []
        assert "downstream_load" in _skipped_steps(result)

    def test_wait_error_fails_load_task(self, monkeypatch):
        from dagster_gcp.dataproc.types import DataprocError

        _patch_client(monkeypatch, wait_error=DataprocError("Job error: ERROR"))
        job = _make_job({})
        node = job.sl_load(
            task_id=PRELOAD_TASK_ID,  # reuse the RUN_CONFIG op name
            domain="starbake",
            table="customers",
            retries=0,  # a retry policy would add a real 300s delay here
        )
        result = _execute(node, raise_on_error=False)

        assert not result.success
        failures = _step_failures(result)
        assert len(failures) == 1
        assert "did not succeed" in str(failures[0].event_specific_data.error)

    def test_terminal_non_done_state_routes_to_failure_branch(self, monkeypatch):
        # wait_for_job returns normally but get_job reports a non-DONE
        # terminal state — must take the failure branch (skip for preload)
        submitted, waited = _patch_client(monkeypatch, get_state="CANCELLED")
        node = _make_preload_node(_make_job({"pre_load_strategy": "imported"}))
        result, downstream_calls = _execute_with_downstream(node)

        assert result.success
        assert len(submitted) == 1
        assert len(waited) == 1
        assert downstream_calls == []
        assert "downstream_load" in _skipped_steps(result)

    def test_retry_within_run_resubmits_fresh_id_with_intact_arguments(
        self, monkeypatch
    ):
        # THE issue-#109 retry scenario: attempt 1 fails, the RetryPolicy
        # re-executes the op IN the same run — the re-submission must carry a
        # fresh job id (Dataproc rejects duplicates) AND an uncorrupted
        # argument vector (issue #111: arguments.pop(0) used to mutate the op
        # closure, so attempt 2 lost its command verb)
        from dagster_gcp.dataproc.types import DataprocError

        submitted, waited = _patch_client(
            monkeypatch, wait_plan=[DataprocError("Job error: ERROR"), None]
        )
        job = _make_job({"retry_delay": "1"})
        node = job.sl_load(
            task_id=PRELOAD_TASK_ID,  # reuse the RUN_CONFIG op name
            domain="starbake",
            table="customers",
            retries=1,
        )
        result = _execute(node, raise_on_error=False)

        assert result.success
        assert len(submitted) == 2
        first, second = _job_ids(submitted)
        assert first != second
        args_per_attempt = [
            details["job"]["spark_job"]["args"] for details in submitted
        ]
        assert args_per_attempt[0] == args_per_attempt[1]
        assert args_per_attempt[0][0] == "load"

    def test_dry_run_never_submits(self, monkeypatch):
        submitted, waited = _patch_client(monkeypatch)
        node = _make_preload_node(_make_job({"pre_load_strategy": "imported"}))
        result = _execute_dry_run(node)
        assert result.success
        assert submitted == []
        assert waited == []


class TestWaitTimeoutOption:

    def test_option_plumbed_to_wait_for_job(self, monkeypatch):
        submitted, waited = _patch_client(monkeypatch)
        node = _make_preload_node(
            _make_job(
                {"pre_load_strategy": "imported", "dataproc_job_wait_timeout": "120"}
            )
        )
        assert _execute(node).success
        assert [wait_timeout for _, wait_timeout in waited] == [120]

    @pytest.mark.parametrize("value", ["abc", "0", "-5", "+120", "1_000"])
    def test_invalid_values_raise_at_definition_time(self, value):
        job = _make_job(
            {"pre_load_strategy": "imported", "dataproc_job_wait_timeout": value}
        )
        with pytest.raises(ValueError) as exc_info:
            _make_preload_node(job)
        message = str(exc_info.value)
        assert "dataproc_job_wait_timeout" in message
        assert value in message
