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

import ast
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError

# ---------------------------------------------------------------------------
# Template search paths — templates include files from multiple modules
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SNOWFLAKE_RESOURCES = (
    _PROJECT_ROOT / "starlake-snowflake" / "src" / "main" / "resources"
)
_ORCH_RESOURCES = (
    _PROJECT_ROOT / "starlake-orchestration" / "src" / "main" / "resources"
)

_SQL_LOAD_TEMPLATE = "templates/dags/load/snowflake__scheduled_table__sql.py.j2"
_SQL_TRANSFORM_TEMPLATE = (
    "templates/dags/transform/snowflake__scheduled_task__sql.py.j2"
)


def _make_mock_context():
    """Build a minimal mock context matching what ``starlake dag-generate`` provides.

    Snowflake SQL templates require additional context fields beyond Dagster:
    ``statements``, ``expectationItems``, ``audit``, ``expectations``,
    ``acl``, and ``pyjson`` (load only).
    """

    class _Option:
        def __init__(self, name, value):
            self.name = name
            self.value = value

    class _Table:
        def __init__(self, name):
            self.name = name
            self.final_name = name

    class _Domain:
        def __init__(self, name, tables):
            self.name = name
            self.final_name = name
            self.tables = [_Table(t) for t in tables]

    class _Schedule:
        def __init__(self, schedule, cron, domains):
            self.schedule = schedule
            self.cron = cron
            self.domains = [_Domain(d, tables) for d, tables in domains.items()]

    class _Config:
        def __init__(self):
            self.comment = "Test DAG for loading starbake tables"
            self.template = "load/snowflake__scheduled_table__sql.py.j2"
            self.options = [
                _Option("SL_ROOT", "/tmp/test"),
                _Option("SL_ENV", "DUCKDB"),
                _Option("stage_location", "staging"),
                _Option("warehouse", "COMPUTE_WH"),
                _Option("timezone", "UTC"),
            ]

    class _Context:
        def __init__(self):
            self.config = _Config()
            self.cron = "0 0 * * *"
            self.schedules = [
                _Schedule(
                    schedule="daily",
                    cron="0 0 * * *",
                    domains={"starbake": ["customers", "orders"]},
                ),
            ]
            self.dependencies = "[]"
            self.statements = "{}"
            self.expectationItems = "{}"
            self.audit = "{}"
            self.expectations = "{}"
            self.acl = "{}"

    return _Context()


@pytest.fixture(scope="module")
def jinja_env():
    """Jinja2 environment with search paths covering Snowflake and core modules."""
    return Environment(
        loader=FileSystemLoader(
            [str(_SNOWFLAKE_RESOURCES), str(_ORCH_RESOURCES)]
        ),
        keep_trailing_newline=True,
    )


# ------------------------------------------------------------------
# Template syntax validation
# ------------------------------------------------------------------

class TestSnowflakeTemplates:

    def test_load_template_is_valid_jinja2(self, jinja_env):
        """Snowflake load SQL template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SQL_LOAD_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Load template has Jinja2 syntax error: {exc}")

    def test_transform_template_is_valid_jinja2(self, jinja_env):
        """Snowflake transform SQL template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SQL_TRANSFORM_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Transform template has Jinja2 syntax error: {exc}")

    # ------------------------------------------------------------------
    # Render with mock context and validate output is valid Python
    # ------------------------------------------------------------------

    def test_load_template_renders_valid_python(self, jinja_env):
        """Render load template with mock variables and verify valid Python."""
        template = jinja_env.get_template(_SQL_LOAD_TEMPLATE)
        rendered = template.render(context=_make_mock_context(), pyjson="{}")
        try:
            ast.parse(rendered)
        except SyntaxError as exc:
            pytest.fail(
                f"Rendered load template is not valid Python: {exc}\n"
                f"--- rendered output ---\n{rendered[:500]}"
            )

    def test_transform_template_renders_valid_python(self, jinja_env):
        """Render transform template with mock variables and verify valid Python."""
        context = _make_mock_context()
        context.config.comment = "Test DAG for transforming starbake tasks"
        context.config.template = (
            "transform/snowflake__scheduled_task__sql.py.j2"
        )
        template = jinja_env.get_template(_SQL_TRANSFORM_TEMPLATE)
        rendered = template.render(context=context)
        try:
            ast.parse(rendered)
        except SyntaxError as exc:
            pytest.fail(
                f"Rendered transform template is not valid Python: {exc}\n"
                f"--- rendered output ---\n{rendered[:500]}"
            )
