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

from __future__ import annotations

import json

import pytest

from ai.starlake.orchestration import StarlakeSchedule

from tests.shared.base_test_orchestration import BaseTestOrchestration

# Backend payloads mirror the Jinja variables documented in the starlake
# `connection` skill for each backend's application.sl.yml block.  The
# names are OPAQUE to the framework — that is precisely what these tests
# prove.  Values must not contain ',' or '=' (the --options wire format
# splits on both; that CLI-contract limitation is out of scope here).
BACKEND_ENVS = {
    "DUCKDB": {"SL_ENV": "DUCKDB"},
    "BIGQUERY": {
        "SL_ENV": "BIGQUERY",
        "GCP_PROJECT": "test-project",
        "GCP_TOKEN": "dummy-token",
    },
    "SNOWFLAKE": {
        "SL_ENV": "SNOWFLAKE",
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_USER": "test-user",
        "SNOWFLAKE_PASSWORD": "dummy-password",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DB": "TEST_DB",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
    },
    "POSTGRESQL": {
        "SL_ENV": "POSTGRESQL",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE": "starlake",
        "POSTGRES_USER": "test-user",
        "POSTGRES_PASSWORD": "dummy-password",
    },
}

# Markers that would betray orchestrator-side connection resolution if
# they ever surfaced in a built CLI command.
_CONNECTION_MARKERS = (
    "connectionRef",
    "jdbc:",
    "--connection",
    "org.duckdb",
    "org.postgresql",
)


class BaseTestConnectionPortability(BaseTestOrchestration):
    """Abstract base: connection configuration is passed through verbatim.

    'Mocked CLI' here means the Starlake CLI is NEVER executed: the tests
    build tasks through the full orchestration -> pipeline path and inspect
    the command + environment the orchestrator WOULD hand to the CLI.
    """

    def _make_load_task(self, backend: str):
        """Build a load task with the backend's env in sl_env_var."""
        env = BACKEND_ENVS[backend]
        options = {"sl_env_var": json.dumps(env)}
        schedule = StarlakeSchedule(name=None, cron=None, domains=[])
        pipeline = self.create_test_pipeline(schedule=schedule, options=options)
        with pipeline:
            node = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
        assert node is not None
        return node.task, env

    @pytest.mark.parametrize("backend", sorted(BACKEND_ENVS))
    def test_backend_env_passthrough(self, backend):
        """Every sl_env_var key=value reaches --options verbatim."""
        task, env = self._make_load_task(backend)
        args = self.get_task_arguments(task)
        opts = self.get_arg_value(args, "--options")
        for key, value in env.items():
            assert f"{key}={value}" in opts, (
                f"{backend}: expected '{key}={value}' in --options, got: {opts}"
            )

    @pytest.mark.parametrize("backend", sorted(BACKEND_ENVS))
    def test_no_connection_arguments_injected(self, backend):
        """The built command carries NO connection vocabulary of its own."""
        task, _ = self._make_load_task(backend)
        args = self.get_task_arguments(task)
        assert args, "Expected a non-empty argument list"
        joined = " ".join(args)
        for marker in _CONNECTION_MARKERS:
            assert marker not in joined, (
                f"{backend}: connection marker '{marker}' leaked into: {joined}"
            )

    def test_command_identical_across_backends(self):
        """Same load command modulo the opaque --options payload.

        Proves there is no backend-specific transformation of the CLI
        invocation: swapping the target warehouse changes ONLY the
        forwarded environment, never the command structure.
        """

        def canonical(backend):
            task, _ = self._make_load_task(backend)
            args = list(self.get_task_arguments(task))
            if "--options" in args:
                args[args.index("--options") + 1] = "<OPTIONS>"
            return args

        reference = canonical("DUCKDB")
        assert reference, "Expected a non-empty canonical argument list"
        for backend in ("BIGQUERY", "SNOWFLAKE", "POSTGRESQL"):
            assert canonical(backend) == reference, (
                f"Command structure diverged for backend {backend}"
            )
