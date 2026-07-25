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

"""Story 6.12 (issue #122) — Dagster pre-load not-ready sentinel.

Shell + cloud variants: run-time ``<job_name>__<op_name>__<run_id>`` scope substitution
(never Jinja, never mutating the closure ``arguments``), consume-then-signal
verdicts through the EXISTING primitives (optional-output skip, in-op poke
loop), fail-fast on real failures when the sentinel is configured, and
per-engine OFF-mode captures (byte-identical commands when the option is
unset). Consumption goes through core ``default_sentinel_handlers`` — local
for shell (real tmp_path files), injected fakes for the cloud schemes.
"""

from __future__ import annotations

import re
import time

import pytest

from ai.starlake.sentinel import SENTINEL_OPTION, SENTINEL_SCOPE_TOKEN, sanitize_scope

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

PRELOAD_TASK_ID = "check_starbake_incoming_files"

RUN_CONFIG = {
    "ops": {
        PRELOAD_TASK_ID: {
            "config": {
                "logical_datetime": "2026-07-18T00:00:00+00:00",
                "dry_run": False,
            }
        }
    }
}

SENSOR_OPTIONS = {
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "42",
    "pre_load_timeout": "120",
}

CLOUD_RUN_OPTIONS = {
    "pre_load_strategy": "imported",
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
}

FARGATE_OPTIONS = {
    "pre_load_strategy": "imported",
    "aws_cluster_name": "test-cluster",
    "aws_task_definition_name": "test-task-def",
    "aws_task_definition_container_name": "test-container",
    "aws_subnets": "subnet-1",
    "aws_security_groups": "sg-1",
    "aws_region": "eu-west-1",
}


def _shell_module():
    import ai.starlake.dagster.shell.starlake_dagster_shell_job as shell_mod
    return shell_mod


def _cloud_run_module():
    import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as cloud_run_mod
    return cloud_run_mod


def _fargate_module():
    import ai.starlake.dagster.aws.starlake_dagster_fargate_job as fargate_mod
    return fargate_mod


def _sentinel_module():
    import ai.starlake.sentinel as sentinel_mod
    return sentinel_mod


def _make_shell_job(options: dict):
    from ai.starlake.dagster.shell import StarlakeDagsterShellJob
    return StarlakeDagsterShellJob(
        filename="test_dagster_sentinel.py",
        module_name=_DAGSTER_TEST_MODULE_NAME,
        options=options,
    )


def _shell_sentinel_options(tmp_path, extra=None):
    options = {
        "pre_load_strategy": "imported",
        SENTINEL_OPTION: str(tmp_path / "sentinels"),
    }
    options.update(extra or {})
    return options


def _make_preload_node(job):
    return job.sl_pre_load(domain="starbake", tables={"customers"})


def _execute(node, raise_on_error=True, graph_name="preload_sentinel_graph"):
    from dagster import GraphDefinition
    graph = GraphDefinition(name=graph_name, node_defs=[node])
    return graph.execute_in_process(run_config=RUN_CONFIG, raise_on_error=raise_on_error)


def _execute_with_downstream(node, graph_name="preload_sentinel_graph"):
    from dagster import DependencyDefinition, GraphDefinition, In, op

    downstream_calls = []

    @op(ins={"start": In(str)})
    def downstream_load(start):
        downstream_calls.append(start)
        return start

    graph = GraphDefinition(
        name=graph_name,
        node_defs=[node, downstream_load],
        dependencies={
            "downstream_load": {"start": DependencyDefinition(PRELOAD_TASK_ID, "result")}
        },
    )
    return graph.execute_in_process(run_config=RUN_CONFIG), downstream_calls


def _sentinel_arg(shell_command: str) -> str:
    # the shell variant double-quotes the value (space-safe, #51) — strip
    # the quotes to recover the raw path
    match = re.search(r'--notReadySentinel "?([^" ]+)"?', shell_command)
    assert match, f"--notReadySentinel not found in: {shell_command}"
    return match.group(1)


class _FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock


# ---------------------------------------------------------------------------
# 1. Definition-time contracts (scheme gates, closure capture, OFF-mode)
# ---------------------------------------------------------------------------

class TestDefinitionTime:

    def test_shell_rejects_gs_prefix(self):
        job = _make_shell_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        with pytest.raises(ValueError) as exc_info:
            _make_preload_node(job)
        assert "shell" in str(exc_info.value)

    def test_cloud_run_rejects_local_prefix(self, tmp_path):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        job = StarlakeDagsterCloudRunJob(
            filename="test_dagster_sentinel.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=dict(CLOUD_RUN_OPTIONS, **{SENTINEL_OPTION: str(tmp_path)}),
        )
        with pytest.raises(ValueError) as exc_info:
            _make_preload_node(job)
        assert "cloud_run" in str(exc_info.value)

    def test_fargate_rejects_gs_prefix(self):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        job = StarlakeDagsterFargateJob(
            filename="test_dagster_sentinel.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=dict(FARGATE_OPTIONS, **{SENTINEL_OPTION: "gs://bucket/sentinels"}),
        )
        with pytest.raises(ValueError) as exc_info:
            _make_preload_node(job)
        assert "fargate" in str(exc_info.value)

    def test_resolver_pops_kwarg_unconditionally(self):
        from ai.starlake.dagster import StarlakeDagsterJob
        kwargs = {"sentinel_path": None, "retries": 0}
        assert StarlakeDagsterJob._sl_resolve_sentinel(kwargs, ('gs',), 'cloud_run') is None
        assert "sentinel_path" not in kwargs


# ---------------------------------------------------------------------------
# 2. Shell one-shot — verdicts against a REAL local sentinel (tmp_path)
# ---------------------------------------------------------------------------

class TestShellOneShot:

    def _run(self, tmp_path, monkeypatch, touch_sentinel, return_code=0):
        shell_mod = _shell_module()
        commands = []

        def fake_execute(shell_command, **kwargs):
            commands.append(shell_command)
            if touch_sentinel:
                import pathlib
                sentinel = pathlib.Path(_sentinel_arg(shell_command))
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.touch()
            return ("out", return_code)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)
        node = _make_preload_node(_make_shell_job(_shell_sentinel_options(tmp_path)))
        return node, commands

    def test_ready_proceeds(self, tmp_path, monkeypatch):
        node, commands = self._run(tmp_path, monkeypatch, touch_sentinel=False)
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls != []
        assert len(commands) == 1

    def test_not_ready_consumes_and_skips_downstream(self, tmp_path, monkeypatch):
        from dagster._core.events import DagsterEventType
        node, commands = self._run(tmp_path, monkeypatch, touch_sentinel=True)
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls == []
        skipped = [
            event.step_key for event in result.all_events
            if event.event_type == DagsterEventType.STEP_SKIPPED
        ]
        assert "downstream_load" in skipped
        # consume-then-signal: the marker was deleted
        import pathlib
        assert not pathlib.Path(_sentinel_arg(commands[0])).exists()

    def test_cli_crash_fails_despite_skip_or_start(self, tmp_path, monkeypatch):
        """The forced skip_or_start swallow no longer applies in sentinel
        mode — a crashed CLI fails the op."""
        from dagster._core.events import DagsterEventType
        node, _ = self._run(tmp_path, monkeypatch, touch_sentinel=False, return_code=7)
        result = _execute(node, raise_on_error=False)
        assert not result.success
        failures = [
            event for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
            and event.step_key == PRELOAD_TASK_ID
        ]
        assert len(failures) == 1
        assert "real failure" in str(failures[0].event_specific_data.error)

    def test_substitution_uses_job_op_and_run_id_without_token(self, tmp_path, monkeypatch):
        """Issue #142: the op name is part of the scope — the preload ops of
        a multi-table job must NOT share one marker path (see the Airflow
        twin, #137)."""
        node, commands = self._run(tmp_path, monkeypatch, touch_sentinel=False)
        result, _ = _execute_with_downstream(node)
        command = commands[0]
        assert SENTINEL_SCOPE_TOKEN not in command
        sentinel = _sentinel_arg(command)
        run_id = result.dagster_run.run_id
        assert sentinel.endswith(
            f"{sanitize_scope('preload_sentinel_graph__' + PRELOAD_TASK_ID + '__' + run_id)}.notready"
        )

    def test_closure_arguments_not_mutated_across_runs(self, tmp_path, monkeypatch):
        """Two executions of the SAME node must each substitute their own
        run scope — the closure vector keeps the token (6.9/6.10 rule)."""
        node, commands = self._run(tmp_path, monkeypatch, touch_sentinel=False)
        first = _execute(node, graph_name="sentinel_run_a")
        second = _execute(node, graph_name="sentinel_run_b")
        assert len(commands) == 2
        assert first.dagster_run.run_id in commands[0]
        assert second.dagster_run.run_id in commands[1]
        assert commands[0] != commands[1]

    def test_sentinel_value_is_double_quoted_in_command(self, tmp_path, monkeypatch):
        """Review finding — the shell variant joins ONE command string, so
        the sentinel value must be double-quoted (space-safe, #51), like the
        Airflow bash twin."""
        node, commands = self._run(tmp_path, monkeypatch, touch_sentinel=False)
        _execute(node)
        import re as _re
        assert _re.search(r'--notReadySentinel "[^"]+\.notready"', commands[0])

    def test_off_mode_byte_identical_command(self, tmp_path, monkeypatch):
        shell_mod = _shell_module()
        commands = []
        monkeypatch.setattr(
            shell_mod, "execute_shell_command",
            lambda shell_command, **kwargs: commands.append(shell_command) or ("ok", 0),
        )
        for options in (
            {"pre_load_strategy": "imported"},
            {"pre_load_strategy": "imported", SENTINEL_OPTION: ""},
        ):
            node = _make_preload_node(_make_shell_job(options))
            assert _execute(node).success
        assert commands[0] == commands[1]
        assert "notReadySentinel" not in commands[0]


# ---------------------------------------------------------------------------
# 3. Shell poke loop — sentinel-driven poke-again
# ---------------------------------------------------------------------------

class TestShellPokeLoop:

    def test_pokes_while_sentinel_present(self, tmp_path, monkeypatch, fake_clock):
        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            if len(calls) < 3:
                import pathlib
                sentinel = pathlib.Path(_sentinel_arg(shell_command))
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.touch()
            return ("out", 0)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)
        node = _make_preload_node(
            _make_shell_job(_shell_sentinel_options(tmp_path, SENSOR_OPTIONS))
        )
        result = _execute(node)
        assert result.success
        assert len(calls) == 3
        assert fake_clock.sleeps == [42, 42]
        assert result.output_for_node(PRELOAD_TASK_ID, "result") is not None

    def test_poke_loop_fails_fast_on_cli_crash(self, tmp_path, monkeypatch, fake_clock):
        from dagster._core.events import DagsterEventType
        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("boom", 1)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)
        node = _make_preload_node(
            _make_shell_job(_shell_sentinel_options(tmp_path, SENSOR_OPTIONS))
        )
        result = _execute(node, raise_on_error=False)
        assert not result.success
        # fail-fast: ONE call, no poking until timeout
        assert len(calls) == 1
        assert fake_clock.sleeps == []
        failures = [
            event for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
        ]
        assert "real failure" in str(failures[0].event_specific_data.error)

    def test_soft_fail_deadline_still_skips(self, tmp_path, monkeypatch, fake_clock):
        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            import pathlib
            sentinel = pathlib.Path(_sentinel_arg(shell_command))
            sentinel.parent.mkdir(parents=True, exist_ok=True)
            sentinel.touch()
            return ("out", 0)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)
        node = _make_preload_node(_make_shell_job(_shell_sentinel_options(
            tmp_path, dict(SENSOR_OPTIONS, pre_load_sensor_soft_fail="true")
        )))
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls == []
        assert len(calls) == 3  # pokes at t=0/42/84; next would exceed 120


# ---------------------------------------------------------------------------
# 4. Cloud run — substitution, verdict via injected fakes, OFF-mode
# ---------------------------------------------------------------------------

class TestCloudRunSentinel:

    GS_PREFIX = "gs://bucket/sentinels"

    def _make_job(self, extra=None):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        options = dict(CLOUD_RUN_OPTIONS, **{SENTINEL_OPTION: self.GS_PREFIX})
        options.update(extra or {})
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_sentinel.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def _fake_handlers(self, monkeypatch, store):
        seen = []

        def fake_default_handlers(uri):
            seen.append(uri)
            return (lambda p: len(store) > 0 and store.pop() is not None), None

        # consume_sentinel(path, exists, delete): use a paired fake instead
        def fake_default_handlers(uri):  # noqa: F811
            seen.append(uri)

            def exists(path):
                return path in store

            def delete(path):
                store.discard(path)

            return exists, delete

        monkeypatch.setattr(_sentinel_module(), "default_sentinel_handlers", fake_default_handlers)
        return seen

    def test_command_substituted_and_token_free(self, monkeypatch):
        cloud_mod = _cloud_run_module()
        commands = []
        monkeypatch.setattr(
            cloud_mod, "execute_shell_command",
            lambda shell_command, **kwargs: commands.append(shell_command) or ("ok", 0),
        )
        self._fake_handlers(monkeypatch, store=set())
        node = _make_preload_node(self._make_job())
        result = _execute(node)
        assert result.success
        command = commands[0]
        assert SENTINEL_SCOPE_TOKEN not in command
        assert "--notReadySentinel" in command
        assert f"{self.GS_PREFIX}/starbake/" in command
        assert result.dagster_run.run_id in command

    def test_not_ready_consumes_and_skips(self, monkeypatch):
        cloud_mod = _cloud_run_module()
        monkeypatch.setattr(
            cloud_mod, "execute_shell_command",
            lambda shell_command, **kwargs: ("ok", 0),
        )
        # dynamic store: report present on the first probe, then consumed
        probes = []

        def fake_default_handlers(uri):
            def exists(path):
                probes.append(path)
                return len(probes) == 1

            def delete(path):
                probes.append(("deleted", path))

            return exists, delete

        monkeypatch.setattr(_sentinel_module(), "default_sentinel_handlers", fake_default_handlers)
        node = _make_preload_node(self._make_job())
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls == []
        # consume-then-signal: delete happened right after the positive probe
        assert probes[1][0] == "deleted"
        assert SENTINEL_SCOPE_TOKEN not in probes[0]

    def test_failed_execution_fails_fast(self, monkeypatch):
        from dagster._core.events import DagsterEventType
        cloud_mod = _cloud_run_module()
        monkeypatch.setattr(
            cloud_mod, "execute_shell_command",
            lambda shell_command, **kwargs: ("denied", 1),
        )
        self._fake_handlers(monkeypatch, store=set())
        node = _make_preload_node(self._make_job())
        result = _execute(node, raise_on_error=False)
        assert not result.success
        failures = [
            event for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
        ]
        assert "real failure" in str(failures[0].event_specific_data.error)

    def test_off_mode_byte_identical_command(self, monkeypatch):
        cloud_mod = _cloud_run_module()
        commands = []
        monkeypatch.setattr(
            cloud_mod, "execute_shell_command",
            lambda shell_command, **kwargs: commands.append(shell_command) or ("ok", 0),
        )
        for extra in ({SENTINEL_OPTION: ""}, None):
            from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
            options = dict(CLOUD_RUN_OPTIONS)
            options.update(extra or {})
            job = StarlakeDagsterCloudRunJob(
                filename="test_dagster_sentinel.py",
                module_name=_DAGSTER_TEST_MODULE_NAME,
                options=options,
            )
            assert _execute(_make_preload_node(job)).success
        assert commands[0] == commands[1]
        assert "notReadySentinel" not in commands[0]


# ---------------------------------------------------------------------------
# 5. Fargate — substitution into the helper vector, verdict, OFF-mode
# ---------------------------------------------------------------------------

class TestFargateSentinel:

    S3_PREFIX = "s3://bucket/sentinels"

    def _make_job(self, extra=None):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        options = dict(FARGATE_OPTIONS, **{SENTINEL_OPTION: self.S3_PREFIX})
        options.update(extra or {})
        return StarlakeDagsterFargateJob(
            filename="test_dagster_sentinel.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def _patch_fargate(self, monkeypatch, return_code=0):
        from ai.starlake.aws import StarlakeFargateHelper
        fargate_mod = _fargate_module()
        snapshots = []

        def fake_generate_script(self):
            snapshots.append(list(self.arguments))
            return "/nonexistent/fargate-script.sh"

        monkeypatch.setattr(StarlakeFargateHelper, "generate_script", fake_generate_script)
        monkeypatch.setattr(
            fargate_mod, "execute_shell_script",
            lambda **kwargs: ("ok", return_code),
        )
        return snapshots

    def test_arguments_substituted_and_token_free(self, monkeypatch):
        snapshots = self._patch_fargate(monkeypatch)
        probes = []

        def fake_default_handlers(uri):
            return (lambda p: probes.append(p) or False), (lambda p: None)

        monkeypatch.setattr(_sentinel_module(), "default_sentinel_handlers", fake_default_handlers)
        node = _make_preload_node(self._make_job())
        result = _execute(node)
        assert result.success
        flattened = " ".join(snapshots[0])
        assert SENTINEL_SCOPE_TOKEN not in flattened
        assert "--notReadySentinel" in flattened
        assert result.dagster_run.run_id in flattened
        # the polled path matches the substituted CLI arg
        assert probes[0] in snapshots[0]

    def test_not_ready_skips_downstream(self, monkeypatch):
        self._patch_fargate(monkeypatch)
        probes = []

        def fake_default_handlers(uri):
            def exists(path):
                probes.append(path)
                return len(probes) == 1

            return exists, (lambda p: None)

        monkeypatch.setattr(_sentinel_module(), "default_sentinel_handlers", fake_default_handlers)
        node = _make_preload_node(self._make_job())
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls == []

    def test_failed_task_fails_fast(self, monkeypatch):
        from dagster._core.events import DagsterEventType
        self._patch_fargate(monkeypatch, return_code=1)
        node = _make_preload_node(self._make_job())
        result = _execute(node, raise_on_error=False)
        assert not result.success
        failures = [
            event for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
        ]
        assert "real failure" in str(failures[0].event_specific_data.error)

    def test_off_mode_byte_identical_arguments(self, monkeypatch):
        snapshots = self._patch_fargate(monkeypatch)
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        for extra in (None, {SENTINEL_OPTION: "   "}):
            options = dict(FARGATE_OPTIONS)
            options.update(extra or {})
            job = StarlakeDagsterFargateJob(
                filename="test_dagster_sentinel.py",
                module_name=_DAGSTER_TEST_MODULE_NAME,
                options=options,
            )
            assert _execute(_make_preload_node(job)).success
        assert snapshots[0] == snapshots[1]
        assert "--notReadySentinel" not in snapshots[0]


# ---------------------------------------------------------------------------
# 6. Dataproc — guarded on dagster_gcp (CI installs no dagster-gcp)
# ---------------------------------------------------------------------------

class TestDataprocSentinel:

    DATAPROC_OPTIONS = {
        "pre_load_strategy": "imported",
        "dataproc_project_id": "test-project",
        "dataproc_region": "europe-west1",
        "spark_jar_list": "gs://bucket/starlake.jar",
        "spark_bucket": "test-bucket",
        "spark_job_main_class": "ai.starlake.job.Main",
        "cluster_config_name": "test_cluster",
    }

    def _make_job(self, extra=None):
        pytest.importorskip("dagster_gcp")
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
        options = dict(self.DATAPROC_OPTIONS)
        options.update(extra or {})
        return StarlakeDagsterDataprocJob(
            filename="test_dagster_sentinel.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options,
        )

    def test_local_prefix_rejected(self, tmp_path):
        job = self._make_job({SENTINEL_OPTION: str(tmp_path)})
        with pytest.raises(ValueError) as exc_info:
            _make_preload_node(job)
        assert "dataproc" in str(exc_info.value)

    def test_submitted_args_substituted_and_verdict_applied(self, monkeypatch):
        job = self._make_job({SENTINEL_OPTION: "gs://bucket/sentinels"})
        submitted = []

        class _FakeClient:
            def submit_job(self, job_details):
                import copy
                submitted.append(copy.deepcopy(job_details))

            def wait_for_job(self, job_id, wait_timeout=None):
                return None

            def get_job(self, job_id):
                return {"status": {"state": "DONE"}}

        monkeypatch.setattr(type(job), "__client__", lambda self: _FakeClient())
        probes = []

        def fake_default_handlers(uri):
            def exists(path):
                probes.append(path)
                return len(probes) == 1

            return exists, (lambda p: None)

        monkeypatch.setattr(_sentinel_module(), "default_sentinel_handlers", fake_default_handlers)
        node = _make_preload_node(job)
        result, downstream_calls = _execute_with_downstream(node)
        assert result.success
        assert downstream_calls == []  # first run: not ready → skip
        args = submitted[0]["job"]["spark_job"]["args"]
        flattened = " ".join(args)
        assert SENTINEL_SCOPE_TOKEN not in flattened
        assert "--notReadySentinel" in flattened

    def test_failed_job_fails_fast(self, monkeypatch):
        from dagster._core.events import DagsterEventType
        job = self._make_job({SENTINEL_OPTION: "gs://bucket/sentinels"})

        class _FakeClient:
            def submit_job(self, job_details):
                return None

            def wait_for_job(self, job_id, wait_timeout=None):
                return None

            def get_job(self, job_id):
                return {"status": {"state": "ERROR", "details": "boom"}}

        monkeypatch.setattr(type(job), "__client__", lambda self: _FakeClient())
        node = _make_preload_node(job)
        result = _execute(node, raise_on_error=False)
        assert not result.success
        failures = [
            event for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
        ]
        assert "real failure" in str(failures[0].event_specific_data.error)

    def test_off_mode_payload_identical(self, monkeypatch):
        submitted = []

        class _FakeClient:
            def submit_job(self, job_details):
                import copy
                submitted.append(copy.deepcopy(job_details))

            def wait_for_job(self, job_id, wait_timeout=None):
                return None

            def get_job(self, job_id):
                return {"status": {"state": "DONE"}}

        for extra in (None, {SENTINEL_OPTION: ""}):
            job = self._make_job(extra)
            monkeypatch.setattr(type(job), "__client__", lambda self: _FakeClient())
            assert _execute(_make_preload_node(job)).success
        first_args = submitted[0]["job"]["spark_job"]["args"]
        second_args = submitted[1]["job"]["spark_job"]["args"]
        assert first_args == second_args
        assert "--notReadySentinel" not in first_args
