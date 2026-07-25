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

"""Story 6.12 (issue #122) — Airflow pre-load not-ready sentinel.

Runs on BOTH Airflow majors. The bash wrappers are tested by EXECUTING them
with a stub ``starlake`` script on PATH (flat-wrapper rule 6.4: the wrapper
content is a bash contract, not a string). Cloud coverage is provider-free
where the seams allow (verdict helpers, token-leak pins, the generic cloud
sensor) and provider-guarded for the real per-engine constructions.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME, AIRFLOW_AVAILABLE

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Requires Apache Airflow",
)

try:
    import airflow.providers.amazon.aws.operators.ecs  # noqa: F401
    AMAZON_AVAILABLE = True
except Exception:
    AMAZON_AVAILABLE = False

try:
    import airflow.providers.google.cloud.operators.cloud_run  # noqa: F401
    import google.cloud.run_v2  # noqa: F401
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False

amazon_only = pytest.mark.skipif(
    not AMAZON_AVAILABLE, reason="Requires apache-airflow-providers-amazon"
)
google_only = pytest.mark.skipif(
    not GOOGLE_AVAILABLE, reason="Requires apache-airflow-providers-google"
)

from ai.starlake.sentinel import (  # noqa: E402  (import-light core module)
    SENTINEL_OPTION,
    SENTINEL_SCOPE_TOKEN,
    sanitize_scope,
)

HOSTILE_SCOPE = "manual run '; touch \"$PWD/pwned.canary\"; $(id)"

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
    "pre_load_strategy": "imported",
}

FARGATE_OPTIONS = {
    "aws_cluster_name": "test-cluster",
    "aws_task_definition_name": "test-task-def",
    "aws_task_definition_container_name": "test-container",
    "pre_load_strategy": "imported",
}

DATAPROC_OPTIONS = {
    "pre_load_strategy": "imported",
    "dataproc_project_id": "test-project",
    "dataproc_region": "europe-west1",
    "spark_jar_list": "gs://bucket/starlake.jar",
    "spark_bucket": "test-bucket",
    "spark_job_main_class": "ai.starlake.job.Main",
}


def _make_bash_job(options: dict):
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    return StarlakeAirflowBashJob(
        filename="test_airflow_sentinel.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options=options,
    )


def _bash_sentinel_options(tmp_path, extra=None):
    options = {
        "pre_load_strategy": "imported",
        "sl_env_var": json.dumps({"SL_ROOT": str(tmp_path)}),
        SENTINEL_OPTION: str(tmp_path / "sentinels"),
    }
    options.update(extra or {})
    return options


def _write_stub_starlake(tmp_path) -> str:
    """Stub CLI honoring the --notReadySentinel contract via env knobs:
    STUB_TOUCH_SENTINEL=1 → write the marker; STUB_EXIT_CODE → exit code."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "starlake"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        "sentinel=\"\"\n"
        "prev=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  if [ \"$prev\" = \"--notReadySentinel\" ]; then sentinel=\"$arg\"; fi\n"
        "  prev=\"$arg\"\n"
        "done\n"
        "if [ -n \"$STUB_TOUCH_SENTINEL\" ] && [ -n \"$sentinel\" ]; then\n"
        "  mkdir -p \"$(dirname \"$sentinel\")\"\n"
        "  : > \"$sentinel\"\n"
        "fi\n"
        "exit \"${STUB_EXIT_CODE:-0}\"\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    return str(bin_dir)


def _run_wrapper(command: str, tmp_path, scope: str, touch_sentinel: bool, exit_code: int = 0):
    env = {
        "PATH": f"{_write_stub_starlake(tmp_path)}:/usr/bin:/bin",
        "SL_SENTINEL_SCOPE": scope,
        "STUB_EXIT_CODE": str(exit_code),
        "HOME": str(tmp_path),
    }
    if touch_sentinel:
        env["STUB_TOUCH_SENTINEL"] = "1"
    return subprocess.run(
        ["bash", "-c", command],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _fake_context(dag_id="my_dag", task_id="wait_for", run_id="scheduled__2026-07-18T00:00:00+00:00", try_number=1, max_tries=4):
    return {
        "ti": SimpleNamespace(
            dag_id=dag_id,
            task_id=task_id,
            try_number=try_number,
            max_tries=max_tries,
            xcom_push=lambda **kwargs: None,
        ),
        "run_id": run_id,
    }


def _dag():
    from datetime import datetime
    from airflow import DAG
    return DAG(dag_id="test_sentinel_dag", start_date=datetime(2024, 1, 1), schedule=None)


# ---------------------------------------------------------------------------
# 1. Bash one-shot — construction contract
# ---------------------------------------------------------------------------

class TestBashOneShotConstruction:

    def test_off_mode_is_byte_identical(self, tmp_path):
        """Zero-change guarantee: unset and blank options build the exact
        same operator command as before the feature existed."""
        base_options = {"pre_load_strategy": "imported", "sl_env_var": json.dumps({"SL_ROOT": str(tmp_path)})}
        baseline = _make_bash_job(base_options).sl_pre_load(domain="starbake", tables={"customers"})
        for off_value in (None, "", "   "):
            options = dict(base_options)
            if off_value is not None:
                options[SENTINEL_OPTION] = off_value
            task = _make_bash_job(options).sl_pre_load(domain="starbake", tables={"customers"})
            assert task.bash_command == baseline.bash_command
            assert task.env == baseline.env
        assert "--notReadySentinel" not in baseline.bash_command
        assert "SL_SENTINEL_SCOPE" not in baseline.env

    def test_sentinel_command_shape(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        cmd = task.bash_command
        # flat wrapper rules (6.4)
        assert "bash -c" not in cmd
        assert "set -e" not in cmd
        # the CLI arg and the probe reference the shell variable, not the token
        assert SENTINEL_SCOPE_TOKEN not in cmd
        assert '--notReadySentinel "' in cmd
        assert "${SL_SENTINEL_SCOPE_SAFE}" in cmd
        # sanitizer line present (tr whitelist mirrors sanitize_scope)
        assert "tr -c 'A-Za-z0-9_.+:=-' '_'" in cmd
        # the swallow is REMOVED: a non-zero CLI exit fails the task
        assert "exit $return_code" in cmd
        # env carries the scope as DATA (Jinja renders ids into a value)
        assert task.env["SL_SENTINEL_SCOPE"] == "{{ ti.dag_id }}__{{ ti.task_id }}__{{ run_id }}"
        assert "env" in task.template_fields

    def test_scope_env_value_renders_ids(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path))
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        rendered = task.render_template(
            task.env,
            {"ti": SimpleNamespace(dag_id="my_dag", task_id="wait_for"), "run_id": "run_1"},
        )
        assert rendered["SL_SENTINEL_SCOPE"] == "my_dag__wait_for__run_1"

    def test_gs_prefix_rejected_on_shell(self, tmp_path):
        options = _bash_sentinel_options(tmp_path)
        options[SENTINEL_OPTION] = "gs://bucket/sentinels"
        job = _make_bash_job(options)
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "shell" in str(exc_info.value)

    def test_non_preload_tasks_untouched(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path))
        task = job.sl_load(task_id="load_starbake_customers", domain="starbake", table="customers")
        assert "notReadySentinel" not in task.bash_command
        assert "SL_SENTINEL_SCOPE" not in (task.env or {})


# ---------------------------------------------------------------------------
# 2. Bash one-shot — executed wrapper contract (stub CLI)
# ---------------------------------------------------------------------------

class TestBashOneShotWrapperExecution:

    def _one_shot_command(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path))
        return job.sl_pre_load(domain="starbake", tables={"customers"}).bash_command

    def test_ready_echoes_zero(self, tmp_path):
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, "dag__run1", touch_sentinel=False)
        assert result.returncode == 0
        assert result.stdout.strip().splitlines()[-1] == "0"

    def test_not_ready_consumes_and_echoes_one(self, tmp_path):
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, "dag__run1", touch_sentinel=True)
        assert result.returncode == 0
        assert result.stdout.strip().splitlines()[-1] == "1"
        # consume-then-signal: the marker was deleted by the wrapper
        assert not (tmp_path / "sentinels" / "starbake" / "dag__run1.notready").exists()

    def test_cli_crash_fails_the_task(self, tmp_path):
        """The 6.3 preload swallow is REMOVED in sentinel mode."""
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, "dag__run1", touch_sentinel=False, exit_code=7)
        assert result.returncode == 7

    def test_cli_exit_99_remapped_to_real_failure(self, tmp_path):
        """Review finding — BashOperator's default skip_on_exit_code=99
        would green-skip a crash; the wrapper remaps 99 → 1."""
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, "dag__run1", touch_sentinel=False, exit_code=99)
        assert result.returncode == 1
        assert "remapped" in result.stdout

    def test_cli_exit_2_is_not_special_in_one_shot(self, tmp_path):
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, "dag__run1", touch_sentinel=False, exit_code=2)
        assert result.returncode == 2

    def test_hostile_scope_is_sanitized_not_executed(self, tmp_path):
        result = _run_wrapper(self._one_shot_command(tmp_path), tmp_path, HOSTILE_SCOPE, touch_sentinel=True)
        assert result.returncode == 0
        assert result.stdout.strip().splitlines()[-1] == "1"
        # no injection: the canary was never created
        assert not (tmp_path / "pwned.canary").exists()
        # the marker landed (and was consumed) under the SANITIZED name
        expected = tmp_path / "sentinels" / "starbake" / f"{sanitize_scope(HOSTILE_SCOPE)}.notready"
        assert not expected.exists()
        # sanity: the sanitized scope carries no shell metacharacters
        for char in " '\"$();":
            assert char not in sanitize_scope(HOSTILE_SCOPE)


# ---------------------------------------------------------------------------
# 3. Bash sensor mode — closed {0,1,2} contract
# ---------------------------------------------------------------------------

class TestBashSensorMode:

    SENSOR_EXTRA = {
        "pre_load_sensor": "true",
        "pre_load_poke_interval": "42",
        "pre_load_timeout": "120",
    }

    def _sensor(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path, self.SENSOR_EXTRA))
        return job.sl_pre_load(domain="starbake", tables={"customers"})

    def test_sensor_construction(self, tmp_path):
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakePreloadBashSensor
        from ai.starlake.airflow.compat import supports_bash_retry_exit_code
        sensor = self._sensor(tmp_path)
        assert isinstance(sensor, StarlakePreloadBashSensor)
        # 6.2 contracts intact
        assert sensor.poke_interval == 42
        assert sensor.timeout == 120
        assert sensor.mode == "reschedule"
        assert sensor.retries == 0
        # 6.12 — closed exit-code contract. retry_exit_code is an Airflow 2.10+
        # BashSensor parameter; below 2.10 it is dropped so the sensor still
        # constructs (issue #125).
        expected_rec = 2 if supports_bash_retry_exit_code() else None
        assert getattr(sensor, "retry_exit_code", None) == expected_rec
        assert sensor.env["SL_SENTINEL_SCOPE"] == "{{ ti.dag_id }}__{{ ti.task_id }}__{{ run_id }}"
        cmd = sensor.bash_command
        assert f'cd "{str(tmp_path)}" &&' in cmd
        assert "exit 2" in cmd and "exit 0" in cmd and "exit 1" in cmd
        assert SENTINEL_SCOPE_TOKEN not in cmd

    def test_caller_retry_exit_code_override_is_ignored(self, tmp_path):
        """Review finding — the closed {0,1,2} contract owns retry_exit_code:
        a caller override would invert real-failure vs poke-again."""
        from ai.starlake.airflow.compat import supports_bash_retry_exit_code
        job = _make_bash_job(_bash_sentinel_options(tmp_path, self.SENSOR_EXTRA))
        sensor = job.sl_pre_load(domain="starbake", tables={"customers"}, retry_exit_code=1)
        # Below 2.10 there is no retry_exit_code param at all, so the caller
        # value is dropped (not honoured either) — "ignored" holds on both.
        expected_rec = 2 if supports_bash_retry_exit_code() else None
        assert getattr(sensor, "retry_exit_code", None) == expected_rec

    def test_sensor_without_sentinel_unchanged(self, tmp_path):
        """6.2 regression pin — sensor mode without the sentinel option keeps
        the raw command and retry_exit_code=None."""
        options = {
            "pre_load_strategy": "imported",
            "sl_env_var": json.dumps({"SL_ROOT": str(tmp_path)}),
        }
        options.update(self.SENSOR_EXTRA)
        sensor = _make_bash_job(options).sl_pre_load(domain="starbake", tables={"customers"})
        # No sentinel → no forced exit-code contract. On 2.10+ the attribute is
        # present and None; below 2.10 the BashSensor has no such attribute.
        assert getattr(sensor, "retry_exit_code", None) is None
        assert "return_code=$?" not in sensor.bash_command

    def test_sensor_wrapper_exit_codes(self, tmp_path):
        cmd = self._sensor(tmp_path).bash_command
        # ready → 0 (done)
        assert _run_wrapper(cmd, tmp_path, "dag__run1", touch_sentinel=False).returncode == 0
        # not ready → 2 (poke again) + marker consumed
        not_ready = _run_wrapper(cmd, tmp_path, "dag__run1", touch_sentinel=True)
        assert not_ready.returncode == 2
        assert not (tmp_path / "sentinels" / "starbake" / "dag__run1.notready").exists()
        # CLI crash → 1 (real failure) — even a CLI exiting 2 maps to 1
        assert _run_wrapper(cmd, tmp_path, "dag__run1", touch_sentinel=False, exit_code=7).returncode == 1
        assert _run_wrapper(cmd, tmp_path, "dag__run1", touch_sentinel=False, exit_code=2).returncode == 1

    def test_sensor_hostile_scope(self, tmp_path):
        cmd = self._sensor(tmp_path).bash_command
        result = _run_wrapper(cmd, tmp_path, HOSTILE_SCOPE, touch_sentinel=True)
        assert result.returncode == 2
        assert not (tmp_path / "pwned.canary").exists()


# ---------------------------------------------------------------------------
# 4. skip_or_start composition — the echoed verdict drives the gate
# ---------------------------------------------------------------------------

class TestSkipOrStartComposition:

    def test_verdict_echo_drives_the_gate(self, tmp_path):
        job = _make_bash_job(_bash_sentinel_options(tmp_path))
        preload = job.sl_pre_load(domain="starbake", tables={"customers"})
        short_circuit = job.skip_or_start_op(
            task_id="skip_or_start_loading_starbake", upstream_task=preload
        )
        f_skip_or_start = short_circuit.python_callable
        ti = MagicMock()
        ti.xcom_pull.return_value = "0"  # ready
        assert f_skip_or_start(preload.task_id, ti=ti) is True
        ti.xcom_pull.return_value = "1"  # not ready
        assert f_skip_or_start(preload.task_id, ti=ti) is False


# ---------------------------------------------------------------------------
# 5. Provider-free cloud seams
# ---------------------------------------------------------------------------

class TestSentinelScopeParts:

    def test_scope_parts_from_context(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        dag_id, task_id, run_id = StarlakeAirflowJob._sl_sentinel_scope_parts(_fake_context())
        assert dag_id == "my_dag"
        assert task_id == "wait_for"
        assert run_id == "scheduled__2026-07-18T00:00:00+00:00"

    def test_missing_run_id_raises(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowException):
            StarlakeAirflowJob._sl_sentinel_scope_parts(
                {"ti": SimpleNamespace(dag_id="d", task_id="t")}
            )

    def test_missing_task_id_raises(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowException):
            StarlakeAirflowJob._sl_sentinel_scope_parts(
                {"ti": SimpleNamespace(dag_id="d"), "run_id": "r"}
            )

    def test_scope_is_task_unique_within_one_run(self):
        """Issue #137 pin: the sensors of a multi-table domain belong to ONE
        dag run — their substituted sentinel paths MUST differ (a shared path
        lets one wrapper consume the other's not-ready marker under a
        concurrent executor, turning a not-ready table into a false READY)."""
        from ai.starlake.airflow import StarlakeAirflowJob
        from ai.starlake.sentinel import substitute_scope
        path = f"file:///sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"
        paths = {
            substitute_scope(
                path,
                *StarlakeAirflowJob._sl_sentinel_scope_parts(
                    _fake_context(task_id=task_id)
                ),
            )
            for task_id in (
                "starbake.stg_t_trs.wait_for",
                "starbake.stg_m_trs.wait_for",
            )
        }
        assert len(paths) == 2


class TestPayloadSubstitution:

    PAYLOAD = {
        "container_overrides": [
            {
                "args": ["preload", "--notReadySentinel", f"gs://b/d/{SENTINEL_SCOPE_TOKEN}.notready"],
                "env": [{"name": "K", "value": "V"}],
            }
        ]
    }

    def test_token_leak_pin_and_non_mutation(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        substituted = StarlakeAirflowJob._sl_sentinel_substitute_payload(
            self.PAYLOAD, _fake_context(run_id="manual 'run'")
        )
        flattened = json.dumps(substituted)
        assert SENTINEL_SCOPE_TOKEN not in flattened
        expected_scope = sanitize_scope("my_dag__wait_for__manual 'run'")
        assert f"gs://b/d/{expected_scope}.notready" in flattened
        # the original payload is NOT mutated (per-attempt copies)
        assert SENTINEL_SCOPE_TOKEN in json.dumps(self.PAYLOAD)


class TestSentinelReadyHelper:

    def test_ready_and_consume(self, tmp_path):
        from ai.starlake.airflow import StarlakeAirflowJob
        deleted = []
        path = f"gs://b/d/{SENTINEL_SCOPE_TOKEN}.notready"
        ready = StarlakeAirflowJob._sl_sentinel_ready(
            path, _fake_context(), lambda p: True, deleted.append
        )
        assert ready is False
        assert deleted == ["gs://b/d/my_dag__wait_for__scheduled__2026-07-18T00:00:00+00:00.notready"]
        assert StarlakeAirflowJob._sl_sentinel_ready(
            path, _fake_context(), lambda p: False, deleted.append
        ) is True
        assert len(deleted) == 1


class TestDeferrableSentinelVerdicts:

    def _wait(self, soft_fail=False):
        from ai.starlake.airflow import PreLoadWait
        from datetime import timedelta
        return PreLoadWait(
            mode="deferrable", poke_interval=30, timeout=120,
            soft_fail=soft_fail, retries=4, retry_delay=timedelta(seconds=30),
        )

    def test_ready_returns_true(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_sentinel_deferrable_success(
            _fake_context(), self._wait(), "t", "gs://b/d/x", lambda p: False, lambda p: None
        ) is True

    def test_not_ready_within_window_raises_retryable(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowException):
            StarlakeAirflowJob._sl_sentinel_deferrable_success(
                _fake_context(try_number=1, max_tries=4),
                self._wait(), "t", "gs://b/d/x", lambda p: True, lambda p: None,
            )

    def test_not_ready_last_attempt_soft_fail_skips(self):
        from ai.starlake.airflow.compat import AirflowSkipException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowSkipException):
            StarlakeAirflowJob._sl_sentinel_deferrable_success(
                _fake_context(try_number=5, max_tries=4),
                self._wait(soft_fail=True), "t", "gs://b/d/x", lambda p: True, lambda p: None,
            )

    def test_engine_failure_fails_fast(self):
        from airflow.exceptions import AirflowFailException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowFailException):
            StarlakeAirflowJob._sl_sentinel_engine_failure("t", RuntimeError("boom"))


class TestCloudPreloadSensorSentinel:

    def _sensor(self, store, submit_result=True, submit_error=None, captured=None):
        from ai.starlake.airflow.starlake_airflow_job import StarlakeCloudPreloadSensor

        def submit_and_wait(context, payload):
            if captured is not None:
                captured.append(payload)
            if submit_error is not None:
                raise submit_error
            return submit_result

        def handlers():
            return (lambda p: p in store), (lambda p: store.discard(p))

        return StarlakeCloudPreloadSensor(
            task_id="preload_sensor",
            dataset=None,
            source=None,
            submit_and_wait=submit_and_wait,
            payload={"args": ["preload", "--notReadySentinel", f"gs://b/d/{SENTINEL_SCOPE_TOKEN}.notready"]},
            sentinel_path=f"gs://b/d/{SENTINEL_SCOPE_TOKEN}.notready",
            sentinel_handlers=handlers,
            poke_interval=1,
            timeout=10,
        )

    def test_ready_completes_truthy(self):
        from ai.starlake.airflow.compat import PokeReturnValue
        verdict = self._sensor(store=set()).poke(_fake_context())
        assert isinstance(verdict, PokeReturnValue)
        assert verdict.xcom_value is True

    def test_not_ready_consumes_and_pokes_again(self):
        expected = "gs://b/d/my_dag__wait_for__scheduled__2026-07-18T00:00:00+00:00.notready"
        store = {expected}
        verdict = self._sensor(store=store).poke(_fake_context())
        assert verdict is None  # poke again
        assert store == set()   # consumed

    def test_submitted_payload_has_no_token(self):
        captured = []
        self._sensor(store=set(), captured=captured).poke(_fake_context())
        assert SENTINEL_SCOPE_TOKEN not in json.dumps(captured[0])

    def test_engine_failure_fails_fast_instead_of_poking(self):
        from airflow.exceptions import AirflowFailException
        with pytest.raises(AirflowFailException):
            self._sensor(store=set(), submit_error=RuntimeError("IAM denied")).poke(_fake_context())

    def test_without_sentinel_submission_error_still_pokes(self):
        """6.5 regression pin — sentinel off keeps the poke-again-on-error
        contract."""
        from ai.starlake.airflow.starlake_airflow_job import StarlakeCloudPreloadSensor
        sensor = StarlakeCloudPreloadSensor(
            task_id="preload_sensor",
            dataset=None,
            source=None,
            submit_and_wait=lambda context, payload: (_ for _ in ()).throw(RuntimeError("no files")),
            payload={},
            poke_interval=1,
            timeout=10,
        )
        assert sensor.poke(_fake_context()) is None


# ---------------------------------------------------------------------------
# 6. Wrapper builders — generic executed contracts (provider-free)
# ---------------------------------------------------------------------------

class TestWrapperBuilders:

    def _run(self, script, tmp_path, extra_env=None):
        env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}
        env.update(extra_env or {})
        return subprocess.run(
            ["bash", "-c", script], cwd=str(tmp_path), env=env,
            capture_output=True, text=True, timeout=30,
        )

    def test_sensor_builder_without_sanitize_uses_injected_safe_scope(self, tmp_path):
        """The gcloud flavor: python injects SL_SENTINEL_SCOPE_SAFE directly
        (BashSensor has no append_env) — no tr line in the wrapper."""
        from ai.starlake.airflow import StarlakeAirflowJob
        marker = tmp_path / "scope_value.notready"
        script = StarlakeAirflowJob._sl_sentinel_sensor_command(
            "true",
            f'[ -f "{tmp_path}/${{SL_SENTINEL_SCOPE_SAFE}}.notready" ]',
            f'rm -f "{tmp_path}/${{SL_SENTINEL_SCOPE_SAFE}}.notready"',
            sanitize_env=False,
        )
        assert "tr -c" not in script
        marker.touch()
        result = self._run(script, tmp_path, {"SL_SENTINEL_SCOPE_SAFE": "scope_value"})
        assert result.returncode == 2
        assert not marker.exists()

    def test_one_shot_builder_failure_short_circuits_probe(self, tmp_path):
        from ai.starlake.airflow import StarlakeAirflowJob
        probe_log = tmp_path / "probe.log"
        script = StarlakeAirflowJob._sl_sentinel_wrapped_command(
            "bash -c 'exit 3'",
            f'touch "{probe_log}"; false',
            "true",
        )
        result = self._run(script, tmp_path, {"SL_SENTINEL_SCOPE": "x"})
        assert result.returncode == 3
        # the probe must not run after a real failure
        assert not probe_log.exists()


# ---------------------------------------------------------------------------
# 7. Provider-guarded — cloud_run
# ---------------------------------------------------------------------------

@google_only
class TestCloudRunSentinel:

    GS_OPTIONS = dict(CLOUD_RUN_OPTIONS, **{SENTINEL_OPTION: "gs://bucket/sentinels"})

    def _job(self, options, **kwargs):
        from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
        return StarlakeAirflowCloudRunJob(
            filename="test_airflow_sentinel.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
            **kwargs,
        )

    def test_off_mode_gcloud_sync_byte_identical(self):
        options = dict(CLOUD_RUN_OPTIONS, cloud_run_async="false", use_gcloud="true")
        baseline = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        blank = self._job(dict(options, **{SENTINEL_OPTION: "  "})).sl_pre_load(
            domain="starbake", tables={"customers"}
        )
        assert blank.bash_command == baseline.bash_command
        assert "notReadySentinel" not in baseline.bash_command
        assert "SL_SENTINEL_SCOPE" not in baseline.bash_command

    def test_off_mode_python_sync_payload_identical(self):
        options = dict(CLOUD_RUN_OPTIONS, cloud_run_async="false", use_gcloud="false")
        baseline = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        blank = self._job(dict(options, **{SENTINEL_OPTION: ""})).sl_pre_load(
            domain="starbake", tables={"customers"}
        )
        assert blank.overrides == baseline.overrides
        assert baseline.sentinel_path is None
        assert "notReadySentinel" not in json.dumps(baseline.overrides)

    def test_off_mode_gcloud_async_byte_identical(self):
        options = dict(CLOUD_RUN_OPTIONS, cloud_run_async="true", use_gcloud="true")
        with _dag():
            baseline = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        with _dag():
            blank = self._job(dict(options, **{SENTINEL_OPTION: ""})).sl_pre_load(
                domain="starbake", tables={"customers"}
            )
        baseline_tasks = {t.task_id.split(".")[-1]: t for t in baseline}
        blank_tasks = {t.task_id.split(".")[-1]: t for t in blank}
        assert baseline_tasks.keys() == blank_tasks.keys()
        for name, task in baseline_tasks.items():
            if hasattr(task, "bash_command"):
                assert blank_tasks[name].bash_command == task.bash_command
                assert "SENTINEL" not in task.bash_command

    def test_gcloud_sync_sentinel_wrapper(self):
        options = dict(self.GS_OPTIONS, cloud_run_async="false", use_gcloud="true")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        cmd = task.bash_command
        assert SENTINEL_SCOPE_TOKEN not in cmd
        assert "gcloud storage ls" in cmd and "gcloud storage rm" in cmd
        assert "${SL_SENTINEL_SCOPE_SAFE}" in cmd
        assert "tr -c 'A-Za-z0-9_.+:=-' '_'" in cmd
        assert "exit $return_code" in cmd
        assert task.env["SL_SENTINEL_SCOPE"] == "{{ ti.dag_id }}__{{ ti.task_id }}__{{ run_id }}"
        assert task.append_env is True

    def _run_gcloud_wrapper(self, command, tmp_path, gcs_mode):
        """Execute a gcloud sentinel wrapper with a stub gcloud on PATH.
        gcs_mode: present | absent | error (probe infrastructure failure)."""
        bin_dir = tmp_path / "gcloud-bin"
        bin_dir.mkdir(exist_ok=True)
        stub = bin_dir / "gcloud"
        stub.write_text(
            "#!/usr/bin/env bash\n"
            "if [ \"$1\" = storage ] && [ \"$2\" = ls ]; then\n"
            "  case \"$STUB_GCS_MODE\" in\n"
            "    present) exit 0;;\n"
            "    absent) echo 'ERROR: (gcloud.storage.ls) One or more URLs matched no objects.' >&2; exit 1;;\n"
            "    *) echo 'ERROR: (gcloud.storage.ls) PERMISSION_DENIED: missing storage.objects.list' >&2; exit 1;;\n"
            "  esac\n"
            "fi\n"
            "if [ \"$1\" = storage ] && [ \"$2\" = rm ]; then exit 0; fi\n"
            "exit 0\n"
        )
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
        env = {
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "SL_SENTINEL_SCOPE": "dag__run1",
            "STUB_GCS_MODE": gcs_mode,
            "HOME": str(tmp_path),
        }
        return subprocess.run(
            ["bash", "-c", command], cwd=str(tmp_path), env=env,
            capture_output=True, text=True, timeout=30,
        )

    def test_gcloud_probe_three_state_contract(self, tmp_path):
        """Review HIGH — a probe INFRASTRUCTURE failure (auth/permission)
        must fail loudly, never read as 'marker absent' (false READY)."""
        # a non-empty sl_env_var keeps the pre-existing --update-env-vars
        # fragment balanced (with no env vars it degenerates to a lone quote
        # — a generation quirk that would make bash exit 2 on syntax)
        options = dict(
            self.GS_OPTIONS, cloud_run_async="false", use_gcloud="true",
            sl_env_var=json.dumps({"SL_ROOT": "/tmp/sl"}),
        )
        command = self._job(options).sl_pre_load(
            domain="starbake", tables={"customers"}
        ).bash_command
        ready = self._run_gcloud_wrapper(command, tmp_path, "absent")
        assert ready.returncode == 0
        assert ready.stdout.strip().splitlines()[-1] == "0"
        not_ready = self._run_gcloud_wrapper(command, tmp_path, "present")
        assert not_ready.returncode == 0
        assert not_ready.stdout.strip().splitlines()[-1] == "1"
        probe_error = self._run_gcloud_wrapper(command, tmp_path, "error")
        assert probe_error.returncode == 1
        assert "sentinel probe failed" in probe_error.stdout

    def test_gcloud_async_retry_on_failure_true_rejected(self):
        options = dict(
            self.GS_OPTIONS, cloud_run_async="true", use_gcloud="true", retry_on_failure="true"
        )
        with pytest.raises(ValueError) as exc_info:
            self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert "retry_on_failure" in str(exc_info.value)

    def test_gcloud_async_status_task_gains_sentinel_branch(self):
        options = dict(self.GS_OPTIONS, cloud_run_async="true", use_gcloud="true")
        with _dag():
            group = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        tasks = {t.task_id.split(".")[-1]: t for t in group}
        status = tasks["check_starbake_incoming_files_get_completion_status"]
        assert "gcloud storage ls" in status.bash_command
        assert "gcloud storage rm" in status.bash_command
        assert "exit $return_code" in status.bash_command
        submission = tasks["check_starbake_incoming_files"]
        assert SENTINEL_SCOPE_TOKEN not in submission.bash_command
        assert "${SL_SENTINEL_SCOPE_SAFE}" in submission.bash_command
        assert submission.append_env is True

    def test_gcloud_waiting_sensor_sentinel(self):
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakePreloadBashSensor
        options = dict(
            self.GS_OPTIONS, use_gcloud="true",
            pre_load_sensor="true", pre_load_poke_interval="30", pre_load_timeout="120",
        )
        sensor = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(sensor, StarlakePreloadBashSensor)
        assert sensor.retry_exit_code == 2
        assert sensor._sentinel_scope_in_environ is True
        cmd = sensor.bash_command
        assert "gcloud storage ls" in cmd
        assert "exit 2" in cmd
        # python-side scope injection — no tr line on this flavor
        assert "tr -c" not in cmd
        assert SENTINEL_SCOPE_TOKEN not in cmd

    def test_deferrable_operator_carries_sentinel(self):
        options = dict(
            self.GS_OPTIONS, use_gcloud="false",
            pre_load_sensor="true", pre_load_poke_interval="30", pre_load_timeout="120",
        )
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import CloudRunJobOperator
        assert isinstance(task, CloudRunJobOperator)
        assert task.sentinel_path == f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"
        assert task.deferrable is True

    def test_sync_operator_sentinel_precedence_over_retry_on_failure(self, monkeypatch):
        """retry_on_failure=true normally re-raises AND retry_on_failure=false
        swallows — with the sentinel configured BOTH raise (swallow removed)."""
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        options = dict(self.GS_OPTIONS, cloud_run_async="false", use_gcloud="false", retry_on_failure="false")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert task.sentinel_path is not None

        def boom(self, context):
            raise RuntimeError("execution failed")

        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", boom)
        with pytest.raises(RuntimeError):
            task.execute(_fake_context())

    def test_sync_operator_substitutes_payload_before_submit(self, monkeypatch):
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        options = dict(self.GS_OPTIONS, cloud_run_async="false", use_gcloud="false")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        seen = {}

        def fake_execute(self, context):
            seen["overrides"] = self.overrides
            return "job"

        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", fake_execute)
        monkeypatch.setattr(
            type(task), "_sl_sentinel_hook_handlers",
            lambda self: ((lambda p: False), (lambda p: None)),
        )
        assert task.execute(_fake_context()) is True
        assert SENTINEL_SCOPE_TOKEN not in json.dumps(seen["overrides"])

    def test_sync_operator_not_ready_returns_false(self, monkeypatch):
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        options = dict(self.GS_OPTIONS, cloud_run_async="false", use_gcloud="false")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", lambda self, context: "job")
        expected = "gs://bucket/sentinels/starbake/my_dag__wait_for__scheduled__2026-07-18T00:00:00+00:00.notready"
        store = {expected}
        monkeypatch.setattr(
            type(task), "_sl_sentinel_hook_handlers",
            lambda self: ((lambda p: p in store), (lambda p: store.discard(p))),
        )
        assert task.execute(_fake_context()) is False
        assert store == set()


# ---------------------------------------------------------------------------
# 8. Provider-guarded — dataproc
# ---------------------------------------------------------------------------

@google_only
class TestDataprocSentinel:

    def _cluster(self, options):
        from ai.starlake.airflow.gcp import (
            StarlakeAirflowDataprocCluster,
            StarlakeAirflowDataprocClusterConfig,
        )
        config = StarlakeAirflowDataprocClusterConfig(
            cluster_id="test_cluster",
            dataproc_name=None,
            master_config=None,
            worker_config=None,
            secondary_worker_config=None,
            idle_delete_ttl=None,
            single_node=None,
            options=options,
        )
        return StarlakeAirflowDataprocCluster(cluster_config=config, options=options, pool="default_pool")

    def test_off_mode_payload_identical(self):
        from ai.starlake.job import TaskType
        arguments = ["preload", "--domain", "starbake", "--strategy", "imported"]
        baseline = self._cluster(DATAPROC_OPTIONS).submit_starlake_job(
            task_id="preload_task", arguments=list(arguments), task_type=TaskType.PRELOAD,
        )
        blank = self._cluster(dict(DATAPROC_OPTIONS, **{SENTINEL_OPTION: ""})).submit_starlake_job(
            task_id="preload_task", arguments=list(arguments), task_type=TaskType.PRELOAD,
        )
        assert blank.job["spark_job"]["args"] == baseline.job["spark_job"]["args"]
        assert baseline.sentinel_path is None
        assert "notReadySentinel" not in json.dumps(baseline.job["spark_job"]["args"])

    def test_sentinel_flows_into_operator(self):
        from ai.starlake.job import TaskType
        options = dict(DATAPROC_OPTIONS, **{SENTINEL_OPTION: "gs://bucket/sentinels"})
        task = self._cluster(options).submit_starlake_job(
            task_id="preload_task",
            arguments=["preload", "--domain", "starbake", "--strategy", "imported",
                       "--notReadySentinel", f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"],
            task_type=TaskType.PRELOAD,
            sentinel_path=f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready",
        )
        assert task.preload is True
        assert task.sentinel_path.endswith(".notready")
        # definition-time payload still carries the token — substitution is
        # execute-time (token-leak covered by the substitution test below)
        assert SENTINEL_SCOPE_TOKEN in json.dumps(task.job["spark_job"]["args"])

    def test_s3_prefix_rejected_on_dataproc(self):
        from ai.starlake.job import TaskType
        with pytest.raises(ValueError) as exc_info:
            self._cluster(DATAPROC_OPTIONS).submit_starlake_job(
                task_id="preload_task",
                arguments=["preload"],
                task_type=TaskType.PRELOAD,
                sentinel_path="s3://bucket/sentinels/starbake/x.notready",
            )
        assert "dataproc" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 9. Provider-guarded — fargate
# ---------------------------------------------------------------------------

@amazon_only
class TestFargateSentinel:

    S3_OPTIONS = dict(FARGATE_OPTIONS, **{SENTINEL_OPTION: "s3://bucket/sentinels"})

    def _job(self, options):
        from ai.starlake.airflow.aws import StarlakeAirflowFargateJob
        return StarlakeAirflowFargateJob(
            filename="test_airflow_sentinel.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_off_mode_payload_identical(self):
        options = dict(FARGATE_OPTIONS, fargate_async="false")
        baseline = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        blank = self._job(dict(options, **{SENTINEL_OPTION: "   "})).sl_pre_load(
            domain="starbake", tables={"customers"}
        )
        assert blank.overrides == baseline.overrides
        assert baseline.sentinel_path is None
        assert "notReadySentinel" not in json.dumps(baseline.overrides)

    def test_off_mode_async_payload_identical(self):
        options = dict(FARGATE_OPTIONS, fargate_async="true")
        with _dag():
            baseline = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        with _dag():
            blank = self._job(dict(options, **{SENTINEL_OPTION: ""})).sl_pre_load(
                domain="starbake", tables={"customers"}
            )
        baseline_run = {t.task_id.split(".")[-1]: t for t in baseline}["check_starbake_incoming_files"]
        blank_run = {t.task_id.split(".")[-1]: t for t in blank}["check_starbake_incoming_files"]
        assert blank_run.overrides == baseline_run.overrides
        assert baseline_run.sentinel_path is None

    def test_gs_prefix_rejected_on_fargate(self):
        options = dict(FARGATE_OPTIONS, **{SENTINEL_OPTION: "gs://bucket/sentinels"})
        with pytest.raises(ValueError) as exc_info:
            self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert "fargate" in str(exc_info.value)

    def test_sync_operator_carries_sentinel_and_substitutes(self, monkeypatch):
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        options = dict(self.S3_OPTIONS, fargate_async="false")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert task.sentinel_path == f"s3://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"
        seen = {}

        def fake_execute(self, context):
            seen["overrides"] = self.overrides
            return None

        monkeypatch.setattr(EcsRunTaskOperator, "execute", fake_execute)
        monkeypatch.setattr(
            type(task), "_sl_sentinel_hook_handlers",
            lambda self: ((lambda p: False), (lambda p: None)),
        )
        assert task.execute(_fake_context()) is True
        assert SENTINEL_SCOPE_TOKEN not in json.dumps(seen["overrides"])

    def test_sync_operator_failure_raises_with_sentinel(self, monkeypatch):
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        options = dict(self.S3_OPTIONS, fargate_async="false")
        task = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})

        def boom(self, context):
            raise RuntimeError("task failed")

        monkeypatch.setattr(EcsRunTaskOperator, "execute", boom)
        with pytest.raises(RuntimeError):
            task.execute(_fake_context())

    def test_waiting_sensor_flavor_carries_sentinel_and_handlers(self):
        """Review finding 7 — proves the sl_job locals (aws_conn_id) feeding
        the sentinel handler factory exist on the waiting path."""
        from ai.starlake.airflow.starlake_airflow_job import StarlakeCloudPreloadSensor
        options = dict(
            self.S3_OPTIONS,
            pre_load_sensor="true", pre_load_poke_interval="30",
            pre_load_timeout="120", pre_load_deferrable="false",
        )
        sensor = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(sensor, StarlakeCloudPreloadSensor)
        assert sensor._sentinel_path.endswith(".notready")
        assert callable(sensor._sentinel_handlers)

    def test_async_group_construction_with_sentinel(self):
        options = dict(self.S3_OPTIONS, fargate_async="true")
        with _dag():
            group = self._job(options).sl_pre_load(domain="starbake", tables={"customers"})
        tasks = {t.task_id.split(".")[-1]: t for t in group}
        completion = tasks["check_starbake_incoming_files_check_completion"]
        assert completion.sentinel_path.endswith(".notready")
        assert completion.sentinel_aws_conn_id == "aws_default"
        submission = tasks["check_starbake_incoming_files"]
        assert submission.sentinel_path == completion.sentinel_path

    def test_async_completion_sensor_transient_describe_error_pokes_again(self, monkeypatch):
        """Review finding 4 — a describe_tasks hiccup is NOT a run failure:
        with the sentinel configured the sensor pokes again instead of
        fail-fasting (or silently skipping)."""
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor
        sensor = FargateTaskStateSensor(
            task_id="preload_check",
            dataset=None,
            source=None,
            cluster="c",
            task="arn:task",
            preload=True,
            sentinel_path=f"s3://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready",
        )

        def boom(cluster, tasks):
            raise RuntimeError("throttled")

        fake_hook = SimpleNamespace(conn=SimpleNamespace(describe_tasks=boom))
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: fake_hook))
        assert sensor.poke(_fake_context()) is None

    def test_async_completion_sensor_definitive_failure_fails_fast(self, monkeypatch):
        from airflow.exceptions import AirflowFailException
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor
        sensor = FargateTaskStateSensor(
            task_id="preload_check",
            dataset=None,
            source=None,
            cluster="c",
            task="arn:task",
            preload=True,
            sentinel_path=f"s3://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready",
        )
        fake_hook = SimpleNamespace(conn=SimpleNamespace(describe_tasks=lambda cluster, tasks: {
            "tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 3}]}]
        }))
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: fake_hook))
        with pytest.raises(AirflowFailException):
            sensor.poke(_fake_context())

    def test_async_completion_sensor_consumes(self, monkeypatch):
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor
        from ai.starlake.airflow import StarlakeAirflowJob
        from ai.starlake.airflow.compat import PokeReturnValue

        sentinel = f"s3://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"
        sensor = FargateTaskStateSensor(
            task_id="preload_check",
            dataset=None,
            source=None,
            cluster="c",
            task="arn:task",
            preload=True,
            sentinel_path=sentinel,
        )
        expected = "s3://bucket/sentinels/starbake/my_dag__wait_for__scheduled__2026-07-18T00:00:00+00:00.notready"
        store = {expected}
        monkeypatch.setattr(
            StarlakeAirflowJob, "_sl_s3_sentinel_hook_handlers",
            classmethod(lambda cls, aws_conn_id='aws_default': (
                lambda: ((lambda p: p in store), (lambda p: store.discard(p)))
            )),
        )
        fake_hook = SimpleNamespace(conn=SimpleNamespace(describe_tasks=lambda cluster, tasks: {
            "tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 0}]}]
        }))
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: fake_hook))
        verdict = sensor.poke(_fake_context())
        assert isinstance(verdict, PokeReturnValue)
        assert verdict.xcom_value is False  # not ready → skip downstream
        assert store == set()               # consumed
