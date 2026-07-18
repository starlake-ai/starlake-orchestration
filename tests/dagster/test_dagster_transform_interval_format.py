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

"""Story 6.11 (issue #118) — space-free interval bounds on the previous branch.

``get_transform_options`` rendered the ``sl_data_interval_start`` bound of
its ``previous_logical_datetime`` branch through ``unquote_datetime``
(``'T' -> ' '``), producing a value with a SPACE (the end bound stayed
T-form — asymmetric). The single quotes around the values are BY DESIGN
(the CLI consumes ``--options`` as SQL substitution variables); the space
is not: on cloud_run the argument vector is joined into gcloud's
space-separated ``--args "^ ^…"`` fragment, so the ``--options`` value
split into two container argv tokens and the CLI received an unterminated
SQL literal. Shell (double-quoted value), fargate (ECS JSON array) and
dataproc (Spark args list) carried the space harmlessly, and Airflow's
builder has always been space-free — the fix aligns Dagster on the same
``sl_timestamp_format`` for BOTH bounds, on every variant.

The branch fires on every scheduled/sensor-triggered run (RunRequests set
``previous_logical_datetime`` in the ops run config).

The dataproc tests require dagster-gcp (skip-guarded — run in the local
provider venv); the rest runs in CI.
"""

from __future__ import annotations

import types

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME
from tests.dagster.test_dagster_sl_pre_load_cloud_sensor import (
    CLOUD_RUN_OPTIONS,
    DAGSTER_GCP_AVAILABLE,
    DATAPROC_OPTIONS,
    FARGATE_OPTIONS,
    PRELOAD_TASK_ID,
)
from tests.dagster.test_dagster_retry_arguments import patch_fargate

TRANSFORM_NAME = "Kpi.order_summary"

LOGICAL = "2026-07-18T00:00:00+00:00"
PREVIOUS = "2026-07-17T00:00:00+0000"
# the same previous bound as written by the tag/partition encoding
# (quote_datetime: ' '->T, ':'->'.', '+'->'_')
PREVIOUS_TAG_ENCODED = "2026-07-17T00.00.00_0000"

EXPECTED_START = "sl_data_interval_start='2026-07-17T00:00:00+0000'"
EXPECTED_END = "sl_data_interval_end='2026-07-18T00:00:00+0000'"
EXPECTED_OPTIONS = f"{EXPECTED_START},{EXPECTED_END}"

RUN_CONFIG_WITH_PREVIOUS = {
    "ops": {
        PRELOAD_TASK_ID: {
            "config": {
                "logical_datetime": LOGICAL,
                "previous_logical_datetime": PREVIOUS,
                "dry_run": False,
            }
        }
    }
}


def _stub_context():
    """Minimal OpExecutionContext stand-in for get_transform_options: no
    partition, no tags — the logical datetime comes from the config."""

    class _Ctx:
        log = types.SimpleNamespace(
            info=lambda *a, **k: None, warning=lambda *a, **k: None
        )

        @property
        def partition_key(self):
            raise Exception("no partition")

        def get_tag(self, key):
            return None

    return _Ctx()


def _execute_with_previous(node):
    from dagster import GraphDefinition

    graph = GraphDefinition(name="interval_format_graph", node_defs=[node])
    return graph.execute_in_process(run_config=RUN_CONFIG_WITH_PREVIOUS)


def _options_value(arguments):
    for index, arg in enumerate(arguments[:-1]):
        if arg == "--options":
            return arguments[index + 1]
    return None


# ---------------------------------------------------------------------------
# 1. Unit — the previous branch emits both bounds in sl_timestamp_format
# ---------------------------------------------------------------------------

class TestPreviousBranchFormat:

    def _options(self, previous):
        from ai.starlake.dagster import (
            DagsterLogicalDatetimeConfig,
            StarlakeDagsterUtils,
        )

        config = DagsterLogicalDatetimeConfig(
            logical_datetime=LOGICAL,
            previous_logical_datetime=previous,
            dry_run=False,
        )
        return StarlakeDagsterUtils.get_transform_options(
            _stub_context(), config, {}
        )

    def test_both_bounds_space_free_t_form(self):
        # byte-pin: T-form on BOTH bounds (the start used to come out as
        # '2026-07-17 00:00:00+0000' — space form, asymmetric with the end)
        assert self._options(PREVIOUS) == EXPECTED_OPTIONS

    def test_tag_encoded_previous_is_decoded_to_the_same_output(self):
        assert self._options(PREVIOUS_TAG_ENCODED) == EXPECTED_OPTIONS

    def test_fractional_seconds_parse_and_truncate(self):
        # review round: isoformat() emits microseconds by default (the
        # documented manual-recovery escape hatch) — the raw-ISO parse must
        # accept them and sl_timestamp_format truncates the fraction
        assert self._options("2026-07-17T00:00:00.789123+00:00") == EXPECTED_OPTIONS

    def test_non_utc_offset_is_normalized_to_utc(self):
        # both bounds are normalized to UTC (logical_datetime convention)
        assert self._options("2026-07-17T02:00:00+02:00") == EXPECTED_OPTIONS

    def test_naive_previous_still_emits_zoned_space_free_bound(self):
        # a naive value follows the logical_datetime convention (local time
        # normalized to UTC) — machine-local, so pin the SHAPE: zoned,
        # space-free, single quoted pair
        import re

        options = self._options("2026-07-17T00:00:00")
        start = options.split(",")[0]
        assert re.fullmatch(
            r"sl_data_interval_start='\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4}'",
            start,
        )

    def test_garbage_previous_fails_loudly_with_a_named_error(self):
        # pre-review the fuzzy parse either crashed opaquely or silently
        # invented a date — a bad value must name the offending field
        with pytest.raises(ValueError, match="previous_logical_datetime"):
            self._options("not-a-datetime")


# ---------------------------------------------------------------------------
# 2. Cloud Run — the #118 repro: the --options value must stay ONE token
# ---------------------------------------------------------------------------

class TestCloudRunArgsIntegrity:

    def _make_job(self):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_transform_interval_format.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=dict(CLOUD_RUN_OPTIONS),
        )

    def test_options_value_is_a_single_args_token(self, monkeypatch):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        node = self._make_job().sl_transform(
            task_id=PRELOAD_TASK_ID,
            transform_name=TRANSFORM_NAME,
        )
        result = _execute_with_previous(node)

        assert result.success
        assert len(calls) == 1
        fragment = calls[0].split('--args "^ ^', 1)[1].split('"', 1)[0]
        tokens = fragment.split(" ")
        # the ^ ^ fragment is gcloud's container argv, space-separated: the
        # token after --options must carry the WHOLE value (pre-fix the
        # space in the start bound split it into two argv tokens and the
        # CLI received an unterminated SQL literal)
        value = _options_value(tokens)
        assert value == EXPECTED_OPTIONS


# ---------------------------------------------------------------------------
# 3. Cross-variant alignment — shell, fargate (CI) and dataproc (provider)
# ---------------------------------------------------------------------------

class TestShellPreviousFormat:

    def test_shell_ships_t_form_bounds(self, monkeypatch):
        import ai.starlake.dagster.shell.starlake_dagster_shell_job as mod
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 0)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)

        job = StarlakeDagsterShellJob(
            filename="test_dagster_transform_interval_format.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={},
        )
        node = job.sl_transform(
            task_id=PRELOAD_TASK_ID, transform_name=TRANSFORM_NAME
        )
        result = _execute_with_previous(node)

        assert result.success
        # the space was harmless here (the whole --options value is
        # double-quoted) — this pins the cross-variant FORMAT alignment
        assert EXPECTED_START in calls[0]
        assert EXPECTED_END in calls[0]


class TestFargatePreviousFormat:

    def test_fargate_ships_t_form_bounds(self, monkeypatch):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob

        seam = patch_fargate(monkeypatch, [0])
        job = StarlakeDagsterFargateJob(
            filename="test_dagster_transform_interval_format.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=dict(FARGATE_OPTIONS),
        )
        node = job.sl_transform(
            task_id=PRELOAD_TASK_ID, transform_name=TRANSFORM_NAME
        )
        result = _execute_with_previous(node)

        assert result.success
        assert _options_value(seam.seen_arguments[0]) == EXPECTED_OPTIONS


@pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)
class TestDataprocPreviousFormat:

    def test_dataproc_ships_t_form_bounds(self, monkeypatch):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob

        from tests.dagster.test_dagster_dataproc_terminal_state import _patch_client

        submitted, _ = _patch_client(monkeypatch)
        job = StarlakeDagsterDataprocJob(
            filename="test_dagster_transform_interval_format.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=dict(DATAPROC_OPTIONS),
        )
        node = job.sl_transform(
            task_id=PRELOAD_TASK_ID, transform_name=TRANSFORM_NAME
        )
        result = _execute_with_previous(node)

        assert result.success
        shipped = submitted[0]["job"]["spark_job"]["args"]
        assert _options_value(shipped) == EXPECTED_OPTIONS
