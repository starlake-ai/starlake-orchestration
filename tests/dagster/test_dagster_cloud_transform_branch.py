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

"""Story 6.10 (issue #114) — transform-branch defects on the cloud variants.

1. Fargate: the transform branch did ``environment.update(env)`` on
   ``StarlakeFargateHelper.environment`` — a ``List[dict]`` in ECS
   containerOverrides format — so EVERY fargate transform op crashed with
   AttributeError before submission.  The fix merges only the
   transform-derived pairs (transform options + runtime sl_options) into the
   helper's list, preserving the ``[{"name", "value"}]`` format; the local
   ``env`` (a full os.environ copy for the aws-cli subprocess) must NOT leak
   into the container overrides.

2. cloud_run + fargate + dataproc: core ``sl_transform`` appends ``--options``
   only when there ARE options — the transform branches then split/rejoined
   ``command_with_arguments[-1]``, which without ``--options`` is the
   transform NAME: the runtime options were comma-merged into it and the CLI
   received a corrupted ``--name``.  The fix locates ``--options`` and merges
   into its value, appends a fresh ``--options`` when runtime options exist,
   and otherwise leaves the vector untouched.

The dataproc tests require dagster-gcp (skip-guarded — run in the local
provider venv); cloud_run and fargate run in CI.
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
from tests.dagster.test_dagster_retry_arguments import patch_fargate

TRANSFORM_NAME = "Kpi.order_summary"

# a cron in params makes get_transform_options derive the data-interval
# bounds at run time — the runtime-options-present scenario
CRON_PARAMS = {"cron": "0 0 * * *"}


def _name_value(arguments):
    """The argument immediately following --name (None if absent)."""
    for index, arg in enumerate(arguments[:-1]):
        if arg == "--name":
            return arguments[index + 1]
    return None


def _options_value(arguments):
    """The argument immediately following --options (None if absent)."""
    for index, arg in enumerate(arguments[:-1]):
        if arg == "--options":
            return arguments[index + 1]
    return None


def _assert_interval_options(options_value):
    """The derived runtime options are data-interval key=value pairs."""
    assert options_value is not None
    pairs = [opt for opt in options_value.split(",") if opt]
    assert pairs, "expected at least one derived option"
    for pair in pairs:
        assert "=" in pair
    return [pair.split("=")[0] for pair in pairs]


# ---------------------------------------------------------------------------
# 1. Fargate
# ---------------------------------------------------------------------------

class TestFargateTransformBranch:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        return StarlakeDagsterFargateJob(
            filename="test_dagster_cloud_transform_branch.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**FARGATE_OPTIONS, **options},
        )

    def test_transform_without_options_executes_and_keeps_name(self, monkeypatch):
        # RED pre-fix on two counts: environment.update crashed the op, and
        # the option merge rewrote the transform name
        seam = patch_fargate(monkeypatch, [0])
        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
        )
        result = _execute(node)

        assert result.success
        assert len(seam.seen_arguments) == 1
        shipped = seam.seen_arguments[0]
        assert shipped[0] == "transform"
        assert _name_value(shipped) == TRANSFORM_NAME

    def test_transform_with_cron_appends_options_and_merges_environment(
        self, monkeypatch
    ):
        # a sentinel that exists ONLY in os.environ: the container overrides
        # must not inherit the orchestrator's local environment
        monkeypatch.setenv("SL_TEST_LOCAL_ONLY_SENTINEL", "leak")
        seam = patch_fargate(monkeypatch, [0])
        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
            params=dict(CRON_PARAMS),
        )
        result = _execute(node)

        assert result.success
        shipped = seam.seen_arguments[0]
        assert _name_value(shipped) == TRANSFORM_NAME
        derived_keys = _assert_interval_options(_options_value(shipped))

        environment = seam.seen_environments[0]
        # ECS containerOverrides format preserved
        assert all(set(entry) == {"name", "value"} for entry in environment)
        env_names = [entry["name"] for entry in environment]
        # the transform-derived pairs reach the container environment…
        for key in derived_keys:
            assert key in env_names
        # …but the local os.environ does not
        assert "SL_TEST_LOCAL_ONLY_SENTINEL" not in env_names

    def test_transform_with_build_time_options_merges_into_them(self, monkeypatch):
        seam = patch_fargate(monkeypatch, [0])
        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
            transform_options="K1=V1",
            params=dict(CRON_PARAMS),
        )
        result = _execute(node)

        assert result.success
        shipped = seam.seen_arguments[0]
        assert _name_value(shipped) == TRANSFORM_NAME
        # exactly ONE --options argument: the derived pairs merge into the
        # build-time value instead of growing a second flag
        assert shipped.count("--options") == 1
        options_value = _options_value(shipped)
        assert options_value.startswith("K1=V1,")
        _assert_interval_options(options_value)

    def test_transform_runtime_options_equals_value_and_no_env_accumulation(
        self, monkeypatch
    ):
        # review round: runtime sl_option VALUES may contain '=' (JDBC URLs,
        # base64) — the derived-env parse must not crash — and a later run
        # WITHOUT runtime options must not inherit the previous run's pairs
        # (the helper is shared by the op closure)
        import json

        from dagster import GraphDefinition

        seam = patch_fargate(monkeypatch, [0, 0])
        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
        )

        run_config_with_options = {
            "ops": {
                PRELOAD_TASK_ID: {
                    "config": {
                        "logical_datetime": "2026-07-18T00:00:00+00:00",
                        "dry_run": False,
                        "sl_options": json.dumps(
                            {"all": {"jdbc_url": "jdbc:pg://h?user=x"}}
                        ),
                    }
                }
            }
        }
        graph = GraphDefinition(
            name="fargate_runtime_options_graph", node_defs=[node]
        )
        result = graph.execute_in_process(run_config=run_config_with_options)

        assert result.success
        shipped = seam.seen_arguments[0]
        assert _name_value(shipped) == TRANSFORM_NAME
        assert "jdbc_url=jdbc:pg://h?user=x" in _options_value(shipped).split(",")
        env_by_name = {
            entry["name"]: entry["value"] for entry in seam.seen_environments[0]
        }
        assert env_by_name["jdbc_url"] == "jdbc:pg://h?user=x"

        # the local subprocess env carries the runtime pair too (issue #119)
        assert seam.seen_subprocess_envs[0]["jdbc_url"] == "jdbc:pg://h?user=x"

        # second run, no runtime options: neither the container environment
        # (rebuilt from the build-time snapshot) nor the local subprocess env
        # (per-attempt copy, issue #119) may inherit the previous run's pair
        result2 = _execute(node)
        assert result2.success
        env_names_2 = [entry["name"] for entry in seam.seen_environments[1]]
        assert "jdbc_url" not in env_names_2
        assert "jdbc_url" not in seam.seen_subprocess_envs[1]


# ---------------------------------------------------------------------------
# 2. Cloud Run
# ---------------------------------------------------------------------------

class TestCloudRunTransformBranch:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_cloud_transform_branch.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**CLOUD_RUN_OPTIONS, **options},
        )

    def _run_transform(self, monkeypatch, **transform_kwargs):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
            **transform_kwargs,
        )
        result = _execute(node)
        assert result.success
        assert len(calls) == 1
        # the ^ ^ prefix declares the space separator — recover the shipped
        # argument vector from the --args fragment
        fragment = calls[0].split('--args "^ ^', 1)[1].split('"', 1)[0]
        return fragment.split(" ")

    def test_transform_without_options_keeps_name(self, monkeypatch):
        shipped = self._run_transform(monkeypatch)
        assert shipped[0] == "transform"
        # RED pre-fix: the runtime merge rewrote the LAST argument — the
        # transform name — into "Kpi.order_summary,<opts…>"
        assert _name_value(shipped) == TRANSFORM_NAME

    def test_transform_with_cron_appends_options(self, monkeypatch):
        shipped = self._run_transform(monkeypatch, params=dict(CRON_PARAMS))
        assert _name_value(shipped) == TRANSFORM_NAME
        _assert_interval_options(_options_value(shipped))

    def test_transform_with_build_time_options_merges_into_them(self, monkeypatch):
        shipped = self._run_transform(
            monkeypatch, transform_options="K1=V1", params=dict(CRON_PARAMS)
        )
        assert _name_value(shipped) == TRANSFORM_NAME
        assert shipped.count("--options") == 1
        options_value = _options_value(shipped)
        assert options_value.startswith("K1=V1,")
        _assert_interval_options(options_value)

    def test_transform_runtime_options_do_not_accumulate_in_subprocess_env(
        self, monkeypatch
    ):
        # issue #119: the closure env dict fed to the gcloud subprocess must
        # not inherit a previous run's runtime pairs
        import json

        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod
        from dagster import GraphDefinition

        seen_envs = []

        def fake_execute(shell_command, **kwargs):
            seen_envs.append(dict(kwargs.get("env") or {}))
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
        )
        run_config_with_options = {
            "ops": {
                PRELOAD_TASK_ID: {
                    "config": {
                        "logical_datetime": "2026-07-18T00:00:00+00:00",
                        "dry_run": False,
                        "sl_options": json.dumps(
                            {"all": {"jdbc_url": "jdbc:pg://h?user=x"}}
                        ),
                    }
                }
            }
        }
        graph = GraphDefinition(
            name="cloud_run_runtime_env_graph", node_defs=[node]
        )
        assert graph.execute_in_process(
            run_config=run_config_with_options
        ).success
        assert seen_envs[0]["jdbc_url"] == "jdbc:pg://h?user=x"

        assert _execute(node).success
        assert "jdbc_url" not in seen_envs[1]


# ---------------------------------------------------------------------------
# 3. Dataproc
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)
class TestDataprocTransformBranch:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
        return StarlakeDagsterDataprocJob(
            filename="test_dagster_cloud_transform_branch.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**DATAPROC_OPTIONS, **options},
        )

    def _run_transform(self, monkeypatch, **transform_kwargs):
        from tests.dagster.test_dagster_dataproc_terminal_state import _patch_client

        submitted, _ = _patch_client(monkeypatch)
        node = self._make_job({}).sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
            **transform_kwargs,
        )
        result = _execute(node)
        assert result.success
        assert len(submitted) == 1
        return submitted[0]["job"]["spark_job"]["args"]

    def test_transform_without_options_keeps_name(self, monkeypatch):
        shipped = self._run_transform(monkeypatch)
        assert shipped[0] == "transform"
        assert _name_value(shipped) == TRANSFORM_NAME

    def test_transform_with_cron_appends_options(self, monkeypatch):
        shipped = self._run_transform(monkeypatch, params=dict(CRON_PARAMS))
        assert _name_value(shipped) == TRANSFORM_NAME
        _assert_interval_options(_options_value(shipped))

    def test_transform_with_build_time_options_merges_into_them(self, monkeypatch):
        shipped = self._run_transform(
            monkeypatch, transform_options="K1=V1", params=dict(CRON_PARAMS)
        )
        assert _name_value(shipped) == TRANSFORM_NAME
        assert shipped.count("--options") == 1
        options_value = _options_value(shipped)
        assert options_value.startswith("K1=V1,")
        _assert_interval_options(options_value)
