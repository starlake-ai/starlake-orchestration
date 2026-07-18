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

"""Story 6.9 (issue #111) — op bodies must not mutate the closure arguments.

``command = arguments.pop(0)`` mutated the list captured in the op closure,
so a RetryPolicy re-execution popped the NEXT argument as the command verb —
a retried attempt could never succeed.  On Fargate it was worse: the helper
holds the SAME list object and joins it LAZILY (``command``/``overrides``),
so even the FIRST attempt of a non-transform task shipped a container
command missing its verb.  The dataproc leg was fixed in story 6.8; this
story fixes shell, cloud_run and fargate with the same non-mutating read.

All three variants are importable in the CI leg (no dagster-gcp needed).
Retry tests use ``retry_delay: "1"`` — Dagster's retry delay sleeps for
real (~1s per retry).
"""

from __future__ import annotations

import os
import tempfile

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME
from tests.dagster.test_dagster_sl_pre_load_cloud_sensor import (
    CLOUD_RUN_OPTIONS,
    FARGATE_OPTIONS,
    PRELOAD_TASK_ID,
    _execute,
)

TASK_ID = PRELOAD_TASK_ID  # reuse the RUN_CONFIG op name from the helpers

RETRY_OPTIONS = {"retry_delay": "1"}


def patch_fargate(monkeypatch, return_codes):
    """Shared fargate seam (6.9 pattern, reused by the 6.10 test files).

    Patches ``StarlakeFargateHelper.generate_script`` to snapshot what the
    generated script would actually ship (the helper joins ``self.arguments``
    LAZILY) and ``execute_shell_script`` for per-attempt exit codes.  Returns
    a namespace with ``seen_arguments``, ``seen_environments``,
    ``seen_subprocess_envs`` and ``calls``.
    """
    import types

    import ai.starlake.dagster.aws.starlake_dagster_fargate_job as mod
    from ai.starlake.aws import StarlakeFargateHelper

    seam = types.SimpleNamespace(
        seen_arguments=[], seen_environments=[], seen_subprocess_envs=[], calls=[]
    )

    def fake_generate_script(self):
        seam.seen_arguments.append(list(self.arguments))
        seam.seen_environments.append([dict(entry) for entry in self.environment])
        fd, path = tempfile.mkstemp(suffix=".sh", prefix="sl_fargate_seam_")
        os.close(fd)
        return path

    def fake_execute(shell_script_path, **kwargs):
        seam.calls.append(shell_script_path)
        seam.seen_subprocess_envs.append(dict(kwargs.get("env") or {}))
        code = return_codes[min(len(seam.calls), len(return_codes)) - 1]
        return ("out", code)

    monkeypatch.setattr(
        StarlakeFargateHelper, "generate_script", fake_generate_script
    )
    monkeypatch.setattr(mod, "execute_shell_script", fake_execute)
    return seam


def _make_load_node(job):
    return job.sl_load(
        task_id=TASK_ID,
        domain="starbake",
        table="customers",
        retries=1,
    )


# ---------------------------------------------------------------------------
# 1. Shell — identical command across retry attempts
# ---------------------------------------------------------------------------

class TestShellRetryArguments:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        return StarlakeDagsterShellJob(
            filename="test_dagster_retry_arguments.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def test_retry_reexecutes_identical_command(self, monkeypatch):
        import ai.starlake.dagster.shell.starlake_dagster_shell_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 1 if len(calls) == 1 else 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        node = _make_load_node(self._make_job(dict(RETRY_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert result.success
        assert len(calls) == 2
        # the closure list must not be mutated: attempt 2 re-runs the SAME
        # command, verb included (pop(0) used to shift the next arg into the
        # verb position)
        assert calls[0] == calls[1]
        # positional pin: the verb is the first argument after the starlake
        # executable (a substring match could not catch duplication/shifting)
        assert calls[0].split()[1] == "load"


# ---------------------------------------------------------------------------
# 2. Cloud Run — identical gcloud execution across retry attempts
# ---------------------------------------------------------------------------

class TestCloudRunRetryArguments:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_retry_arguments.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**CLOUD_RUN_OPTIONS, **options},
        )

    def test_retry_reexecutes_identical_command(self, monkeypatch):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 1 if len(calls) == 1 else 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        node = _make_load_node(self._make_job(dict(RETRY_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert result.success
        assert len(calls) == 2
        assert calls[0] == calls[1]
        # positional pin: the gcloud --args fragment starts with the verb
        # (its ^ ^ prefix declares the space separator)
        assert '--args "^ ^load ' in calls[0]


# ---------------------------------------------------------------------------
# 3. Fargate — helper arguments intact (verb included) on EVERY attempt
# ---------------------------------------------------------------------------

class TestFargateRetryArguments:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        return StarlakeDagsterFargateJob(
            filename="test_dagster_retry_arguments.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**FARGATE_OPTIONS, **options},
        )

    def _patch_fargate(self, monkeypatch, return_codes):
        seam = patch_fargate(monkeypatch, return_codes)
        return seam.seen_arguments, seam.calls

    def test_first_attempt_container_command_keeps_verb(self, monkeypatch):
        # issue #111 fargate facet: the helper shares the op's arguments
        # list, so pop(0) dropped the verb from the container command on the
        # VERY FIRST attempt of any non-transform task
        seen_arguments, calls = self._patch_fargate(monkeypatch, [0])
        job = self._make_job({})
        node = job.sl_load(
            task_id=TASK_ID,
            domain="starbake",
            table="customers",
            retries=0,
        )
        result = _execute(node)

        assert result.success
        assert len(calls) == 1
        assert len(seen_arguments) == 1
        assert seen_arguments[0][0] == "load"
        assert seen_arguments[0].count("load") == 1

    def test_retry_reexecutes_identical_arguments(self, monkeypatch):
        seen_arguments, calls = self._patch_fargate(monkeypatch, [1, 0])
        node = _make_load_node(self._make_job(dict(RETRY_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert result.success
        assert len(calls) == 2
        assert len(seen_arguments) == 2
        assert seen_arguments[0] == seen_arguments[1]
        assert seen_arguments[0][0] == "load"
        assert seen_arguments[0].count("load") == 1
