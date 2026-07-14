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

import pytest

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_connection_portability import (
    BaseTestConnectionPortability,
)


class TestSnowflakeConnectionPortability(SnowflakeTestMixin, BaseTestConnectionPortability):
    """Snowflake runs Starlake INSIDE Snowflake — the connection model differs.

    The connection from application.sl.yml is consumed by `starlake
    dag-generate` when it compiles the SQL statements embedded in the
    generated DAG module; at runtime the 'connection' is the Snowpark
    Session the DAGTask executes under.  There is no CLI subprocess and
    therefore no env/--options channel to pass connection config through.
    The framework still contains ZERO connection transformation logic
    (see test_snowflake_connection_agnosticism.py) — which is the actual
    portability guarantee this story validates.

    test_no_connection_arguments_injected and
    test_command_identical_across_backends are inherited and PASS: the
    reconstructed task arguments carry no connection vocabulary and are
    identical for every backend payload.
    """

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason=(
            "sl_env_vars are not forwarded to Snowflake task runtime: "
            "sl_job() merges them into a local 'options' dict "
            "(starlake_snowflake_job.py:302-313) that is never used; the "
            "SQL executor's connection is the Snowpark Session, applied "
            "at dag-generate time — see issue #77"
        ),
    )
    @pytest.mark.parametrize(
        "backend", ["BIGQUERY", "DUCKDB", "POSTGRESQL", "SNOWFLAKE"]
    )
    def test_backend_env_passthrough(self, backend):
        super().test_backend_env_passthrough(backend)

    def test_sl_env_vars_not_forwarded_to_task_runtime(self):
        """Documents the current contract: no closure captures the merged
        sl_env_vars.  If the framework ever starts forwarding them (the
        confirmed direction of issue #77), this test fails and the xfail
        above must be revisited together with the issue.
        """
        task, _ = self._make_load_task("BIGQUERY")
        func = task.definition.func
        assert func is not None
        assert "options" not in func.__code__.co_freevars, (
            "Snowflake task closure now captures 'options' — sl_env_vars "
            "forwarding has changed (issue #77); revisit the xfail in "
            "this class"
        )
