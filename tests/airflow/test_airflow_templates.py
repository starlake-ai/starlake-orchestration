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
_AIRFLOW_RESOURCES = _PROJECT_ROOT / "starlake-airflow" / "src" / "main" / "resources"
_ORCH_RESOURCES = _PROJECT_ROOT / "starlake-orchestration" / "src" / "main" / "resources"

_SHELL_LOAD_TEMPLATE = "templates/dags/load/airflow__scheduled_table__shell.py.j2"
_SHELL_TRANSFORM_TEMPLATE = "templates/dags/transform/airflow__scheduled_task__shell.py.j2"


@pytest.fixture(scope="module")
def jinja_env():
    """Jinja2 environment with search paths covering both Airflow and core modules."""
    return Environment(
        loader=FileSystemLoader([str(_AIRFLOW_RESOURCES), str(_ORCH_RESOURCES)]),
        keep_trailing_newline=True,
    )


def _make_mock_context():
    """Build a minimal mock context matching what ``starlake dag-generate`` provides."""

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
            self.template = "load/airflow__scheduled_table__shell.py.j2"
            self.options = [
                _Option("SL_ROOT", "/tmp/test"),
                _Option("SL_ENV", "DUCKDB"),
                _Option("SL_STARLAKE_PATH", "starlake"),
                _Option("pre_load_strategy", "imported"),
                _Option("tags", "starbake"),
            ]

    class _Context:
        def __init__(self):
            self.config = _Config()
            self.sl_airflow_access_control = "None"
            self.cron = "0 0 * * *"
            self.schedules = [
                _Schedule(
                    schedule="daily",
                    cron="0 0 * * *",
                    domains={"starbake": ["customers", "orders", "products"]},
                ),
            ]
            self.dependencies = "[]"

    return _Context()


# ------------------------------------------------------------------
# 4.7  Template syntax validation
# ------------------------------------------------------------------

class TestAirflowTemplates:

    def test_load_template_is_valid_jinja2(self, jinja_env):
        """Airflow load shell template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SHELL_LOAD_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Load template has Jinja2 syntax error: {exc}")

    def test_transform_template_is_valid_jinja2(self, jinja_env):
        """Airflow transform shell template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SHELL_TRANSFORM_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Transform template has Jinja2 syntax error: {exc}")

    def test_load_template_renders_valid_python(self, jinja_env):
        """Render load template with mock variables and verify valid Python."""
        template = jinja_env.get_template(_SHELL_LOAD_TEMPLATE)
        rendered = template.render(context=_make_mock_context())
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
        context.config.template = "transform/airflow__scheduled_task__shell.py.j2"
        template = jinja_env.get_template(_SHELL_TRANSFORM_TEMPLATE)
        rendered = template.render(context=context)
        try:
            ast.parse(rendered)
        except SyntaxError as exc:
            pytest.fail(
                f"Rendered transform template is not valid Python: {exc}\n"
                f"--- rendered output ---\n{rendered[:500]}"
            )
