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

import pytest
from jinja2 import TemplateSyntaxError

from tests.shared.template_test_utils import (
    MODULE_RESOURCES,
    make_jinja_env,
    make_mock_context,
    parse_header_options,
)

_SQL_LOAD_TEMPLATE = "templates/dags/load/snowflake__scheduled_table__sql.py.j2"
_SQL_TRANSFORM_TEMPLATE = (
    "templates/dags/transform/snowflake__scheduled_task__sql.py.j2"
)


@pytest.fixture(scope="module")
def jinja_env():
    """Jinja2 environment with search paths covering Snowflake and core modules."""
    return make_jinja_env("snowflake")


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
        context = make_mock_context(
            "snowflake",
            template="load/snowflake__scheduled_table__sql.py.j2",
        )
        rendered = template.render(context=context, pyjson="{}")
        try:
            ast.parse(rendered)
        except SyntaxError as exc:
            pytest.fail(
                f"Rendered load template is not valid Python: {exc}\n"
                f"--- rendered output ---\n{rendered[:500]}"
            )

    def test_transform_template_renders_valid_python(self, jinja_env):
        """Render transform template with mock variables and verify valid Python."""
        context = make_mock_context(
            "snowflake",
            template="transform/snowflake__scheduled_task__sql.py.j2",
            comment="Test DAG for transforming starbake tasks",
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


# ------------------------------------------------------------------
# Story 3.2 — AC1 snippet composition
# ------------------------------------------------------------------

class TestSnowflakeTemplateComposition:
    """AC1 — snippet composition is visible in the rendered output.

    The Snowflake snippet sets ONLY the orchestrator enum (verified) —
    the negative assertions pin that snippet contract.

    NOTE: the snowflake load template includes ``__common__.py.j2`` a
    SECOND time directly (line 15, besides the include inside the
    orchestrator snippet) — rendered load output contains ``description=``
    /``template=``/``options={...}`` TWICE (later assignment wins; benign).
    Assertions therefore stay substring-based — never single-occurrence.
    """

    def test_load_template_composes_orchestrator_snippet(self, jinja_env):
        context = make_mock_context(
            "snowflake",
            template="load/snowflake__scheduled_table__sql.py.j2",
        )
        rendered = jinja_env.get_template(_SQL_LOAD_TEMPLATE).render(
            context=context, pyjson="{}"
        )
        # __starlake_snowflake_orchestrator.py.j2 — enum ONLY
        assert "orchestrator = StarlakeOrchestrator.SNOWFLAKE" in rendered
        assert "access_control" not in rendered
        assert "default_dag_args" not in rendered
        # __starlake_sql_execution.py.j2 — SQL execution environment
        assert "execution_environment = StarlakeExecutionEnvironment.SQL" in rendered
        assert "statements = {}" in rendered
        assert "expectation_items = {}" in rendered
        assert "audit = {}" in rendered
        assert "expectations = {}" in rendered
        assert "acl = {}" in rendered
        # top-level pyjson render var (load only)
        assert "json_context = '''{}'''" in rendered
        # __common__.py.j2 — config projection
        assert 'description="""Test DAG for loading starbake tables"""' in rendered
        assert 'template="load/snowflake__scheduled_table__sql.py.j2"' in rendered
        assert "'stage_location':'staging'" in rendered
        # inlined pipeline body (the snowflake load template does NOT use
        # load/__scheduled_table_tpl.py.j2 but carries its own copy)
        assert (
            "with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:"
            in rendered
        )

    def test_transform_template_composes_orchestrator_snippet(self, jinja_env):
        context = make_mock_context(
            "snowflake",
            template="transform/snowflake__scheduled_task__sql.py.j2",
            comment="Test DAG for transforming starbake tasks",
        )
        rendered = jinja_env.get_template(_SQL_TRANSFORM_TEMPLATE).render(context=context)
        assert "orchestrator = StarlakeOrchestrator.SNOWFLAKE" in rendered
        assert "execution_environment = StarlakeExecutionEnvironment.SQL" in rendered
        assert "statements = {}" in rendered
        assert "acl = {}" in rendered
        # transform uses the shared task template — no pyjson/json_context
        assert "json_context" not in rendered
        # transform/__scheduled_task_tpl.py.j2 — dependency-driven pipeline logic
        assert 'cron = "0 0 * * *"' in rendered
        assert 'dependencies=StarlakeDependencies(dependencies="""[]"""' in rendered


# ------------------------------------------------------------------
# Story 3.2 — AC2 / NFR2 render idempotence
# ------------------------------------------------------------------

class TestSnowflakeTemplateIdempotence:
    """AC2 / NFR2 — byte-identical re-render at the Python-Jinja2 layer."""

    def _render(self, env, template_name):
        kwargs = {"context": make_mock_context("snowflake", template=template_name)}
        if template_name == _SQL_LOAD_TEMPLATE:
            kwargs["pyjson"] = "{}"
        return env.get_template(template_name).render(**kwargs)

    @pytest.mark.parametrize(
        "template_name", [_SQL_LOAD_TEMPLATE, _SQL_TRANSFORM_TEMPLATE]
    )
    def test_render_is_byte_identical(self, jinja_env, template_name):
        first = self._render(jinja_env, template_name)
        second = self._render(jinja_env, template_name)
        # And from a completely fresh Environment (no loader/cache state):
        third = self._render(make_jinja_env("snowflake"), template_name)
        assert first == second
        assert first.encode("utf-8") == third.encode("utf-8"), (
            f"{template_name} render is not idempotent across environments (NFR2)"
        )


# ------------------------------------------------------------------
# Story 3.2 — AC4 self-documenting headers
# ------------------------------------------------------------------

class TestSnowflakeTemplateHeaderOptions:
    """AC4 — header comments document the options (existing convention).

    CLI-parseability of every ``# - `` line is enforced globally by the
    shared convention test (tests/shared/test_template_conventions.py).
    """

    def test_load_template_header_documents_known_options(self):
        path = (
            MODULE_RESOURCES["snowflake"]
            / "templates" / "dags" / "load" / "snowflake__scheduled_table__sql.py.j2"
        )
        options = parse_header_options(path)
        expected = {
            "stage_location", "sl_incoming_file_stage", "warehouse",
            "timezone", "packages", "sl_env_var",
        }
        missing = expected - set(options)
        assert not missing, f"Load template header no longer documents: {missing}"

    def test_transform_template_header_documents_known_options(self):
        path = (
            MODULE_RESOURCES["snowflake"]
            / "templates" / "dags" / "transform" / "snowflake__scheduled_task__sql.py.j2"
        )
        options = parse_header_options(path)
        expected = {
            "stage_location", "warehouse", "timezone", "packages", "sl_env_var",
        }
        missing = expected - set(options)
        assert not missing, f"Transform template header no longer documents: {missing}"
