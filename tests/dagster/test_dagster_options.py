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

from ai.starlake.common import MissingEnvironmentVariable
from ai.starlake.dagster import DagsterDataset, StarlakeDagsterUtils
from ai.starlake.dagster.shell import StarlakeDagsterShellJob
from ai.starlake.dataset import StarlakeDataset

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME


# ------------------------------------------------------------------
# 4.1  get_context_var resolution chain for Dagster
# ------------------------------------------------------------------

class TestStarlakeOptions:
    """Test StarlakeOptions.get_context_var() resolution for Dagster.

    Dagster inherits the base StarlakeOptions chain: options → default → env var.
    """

    def test_options_dict_takes_precedence(self):
        result = StarlakeDagsterShellJob.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={"my_var": "from_options"},
        )
        assert result == "from_options"

    def test_default_value_second(self):
        result = StarlakeDagsterShellJob.get_context_var(
            var_name="my_var",
            default_value="from_default",
            options={},
        )
        assert result == "from_default"

    def test_env_var_third(self, monkeypatch):
        monkeypatch.setenv("my_var", "from_env")
        result = StarlakeDagsterShellJob.get_context_var(
            var_name="my_var",
            default_value=None,
            options={},
        )
        assert result == "from_env"

    def test_raises_when_nothing_found(self, monkeypatch):
        monkeypatch.delenv("nonexistent_var", raising=False)
        with pytest.raises(MissingEnvironmentVariable):
            StarlakeDagsterShellJob.get_context_var(
                var_name="nonexistent_var",
                default_value=None,
                options={},
            )


# ------------------------------------------------------------------
# 4.2  StarlakeDagsterShellJob constructor
# ------------------------------------------------------------------

class TestStarlakeDagsterShellJob:

    def test_constructor_default_retries(self, dagster_job):
        """Default retries is 1."""
        assert dagster_job.retries == 1

    def test_constructor_default_sl_env_vars(self, dagster_job):
        """Default job has sl_env_vars dict."""
        assert isinstance(dagster_job.sl_env_vars, dict)

    def test_constructor_custom_starlake_path(self):
        """Custom SL_STARLAKE_PATH is accessible via get_context_var."""
        job = StarlakeDagsterShellJob(
            filename="test_custom.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"SL_STARLAKE_PATH": "/custom/starlake"},
        )
        path = job.get_context_var("SL_STARLAKE_PATH", "starlake", job.options)
        assert path == "/custom/starlake"

    def test_constructor_custom_retries(self):
        """Custom retries option is propagated."""
        job = StarlakeDagsterShellJob(
            filename="test_custom.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"retries": "3"},
        )
        assert job.retries == 3

    def test_sl_execution_environment_is_shell(self):
        from ai.starlake.job import StarlakeExecutionEnvironment
        assert StarlakeDagsterShellJob.sl_execution_environment() == StarlakeExecutionEnvironment.SHELL

    def test_sl_orchestrator_is_dagster(self):
        from ai.starlake.job import StarlakeOrchestrator
        assert StarlakeDagsterShellJob.sl_orchestrator() == StarlakeOrchestrator.DAGSTER


# ------------------------------------------------------------------
# 4.3  DagsterDataset.to_event produces AssetKey instances
# ------------------------------------------------------------------

class TestDagsterDataset:

    def test_to_event_produces_asset_key(self):
        from dagster import AssetKey
        ds = StarlakeDataset(name="starbake.customers", cron="0 * * * *")
        event = DagsterDataset.to_event(dataset=ds, source="test_source")
        assert isinstance(event, AssetKey)
        assert ds.uri in event.to_user_string()


# ------------------------------------------------------------------
# 4.4  StarlakeDagsterUtils.quote_datetime / unquote_datetime
# ------------------------------------------------------------------

class TestStarlakeDagsterUtils:

    def test_quote_datetime_round_trip(self):
        original = "2026-03-24 12:00:00+00:00"
        quoted = StarlakeDagsterUtils.quote_datetime(original)
        assert ":" not in quoted
        assert "+" not in quoted
        unquoted = StarlakeDagsterUtils.unquote_datetime(quoted)
        assert unquoted == original

    def test_quote_datetime_none(self):
        assert StarlakeDagsterUtils.quote_datetime(None) is None
        assert StarlakeDagsterUtils.unquote_datetime(None) is None

    def test_quote_datetime_with_T_separator(self):
        """quote replaces ' '→'T', so an original T stays T after quote.
        unquote replaces 'T'→' ', so the round-trip normalises 'T' to ' '."""
        original = "2026-01-15T08:30:00+0000"
        quoted = StarlakeDagsterUtils.quote_datetime(original)
        assert ":" not in quoted
        assert "+" not in quoted
        unquoted = StarlakeDagsterUtils.unquote_datetime(quoted)
        # T → space normalisation is expected behaviour
        assert unquoted == "2026-01-15 08:30:00+0000"
