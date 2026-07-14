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

_SHELL_LOAD_TEMPLATE = "templates/dags/load/dagster__scheduled_table__shell.py.j2"
_SHELL_TRANSFORM_TEMPLATE = "templates/dags/transform/dagster__scheduled_task__shell.py.j2"


@pytest.fixture(scope="module")
def jinja_env():
    """Jinja2 environment with search paths covering both Dagster and core modules."""
    return make_jinja_env("dagster")


# ------------------------------------------------------------------
# 4.7  Template syntax validation
# ------------------------------------------------------------------

class TestDagsterTemplates:

    def test_load_template_is_valid_jinja2(self, jinja_env):
        """Dagster load shell template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SHELL_LOAD_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Load template has Jinja2 syntax error: {exc}")

    def test_transform_template_is_valid_jinja2(self, jinja_env):
        """Dagster transform shell template parses as valid Jinja2."""
        try:
            jinja_env.get_template(_SHELL_TRANSFORM_TEMPLATE)
        except TemplateSyntaxError as exc:
            pytest.fail(f"Transform template has Jinja2 syntax error: {exc}")

    def test_load_template_renders_valid_python(self, jinja_env):
        """Render load template with mock variables and verify valid Python."""
        template = jinja_env.get_template(_SHELL_LOAD_TEMPLATE)
        rendered = template.render(context=make_mock_context("dagster"))
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
            "dagster",
            template="transform/dagster__scheduled_task__shell.py.j2",
            comment="Test DAG for transforming starbake tasks",
        )
        template = jinja_env.get_template(_SHELL_TRANSFORM_TEMPLATE)
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

class TestDagsterTemplateComposition:
    """AC1 — snippet composition is visible in the rendered output.

    The Dagster snippet sets ONLY the orchestrator enum (verified) —
    the negative assertions pin that snippet contract.
    """

    def test_load_template_composes_orchestrator_snippet(self, jinja_env):
        rendered = jinja_env.get_template(_SHELL_LOAD_TEMPLATE).render(
            context=make_mock_context("dagster")
        )
        # __starlake_dagster_orchestrator.py.j2 — enum ONLY
        assert "orchestrator = StarlakeOrchestrator.DAGSTER" in rendered
        assert "access_control" not in rendered
        assert "default_dag_args" not in rendered
        # __starlake_shell_execution.py — execution environment
        assert "execution_environment = StarlakeExecutionEnvironment.SHELL" in rendered
        # __common__.py.j2 — config projection
        assert 'description="""Test DAG for loading starbake tables"""' in rendered
        assert 'template="load/dagster__scheduled_table__shell.py.j2"' in rendered
        assert "'SL_STARLAKE_PATH':'starlake'" in rendered
        # load/__scheduled_table_tpl.py.j2 — shared pipeline logic
        assert (
            "with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:"
            in rendered
        )
        assert "pipelines = [generate_pipeline(schedule) for schedule in schedules]" in rendered

    def test_transform_template_composes_orchestrator_snippet(self, jinja_env):
        context = make_mock_context(
            "dagster",
            template="transform/dagster__scheduled_task__shell.py.j2",
            comment="Test DAG for transforming starbake tasks",
        )
        rendered = jinja_env.get_template(_SHELL_TRANSFORM_TEMPLATE).render(context=context)
        assert "orchestrator = StarlakeOrchestrator.DAGSTER" in rendered
        assert "access_control" not in rendered
        assert "default_dag_args" not in rendered
        assert "execution_environment = StarlakeExecutionEnvironment.SHELL" in rendered
        # transform/__scheduled_task_tpl.py.j2 — dependency-driven pipeline logic
        assert 'cron = "0 0 * * *"' in rendered
        assert 'dependencies=StarlakeDependencies(dependencies="""[]"""' in rendered


# ------------------------------------------------------------------
# Story 3.2 — AC2 / NFR2 render idempotence
# ------------------------------------------------------------------

class TestDagsterTemplateIdempotence:
    """AC2 / NFR2 — byte-identical re-render at the Python-Jinja2 layer."""

    @pytest.mark.parametrize(
        "template_name", [_SHELL_LOAD_TEMPLATE, _SHELL_TRANSFORM_TEMPLATE]
    )
    def test_render_is_byte_identical(self, jinja_env, template_name):
        template = jinja_env.get_template(template_name)
        first = template.render(context=make_mock_context("dagster"))
        second = template.render(context=make_mock_context("dagster"))
        # And from a completely fresh Environment (no loader/cache state):
        third = make_jinja_env("dagster").get_template(template_name).render(
            context=make_mock_context("dagster")
        )
        assert first == second
        assert first.encode("utf-8") == third.encode("utf-8"), (
            f"{template_name} render is not idempotent across environments (NFR2)"
        )


# ------------------------------------------------------------------
# Story 3.2 — AC4 self-documenting headers
# ------------------------------------------------------------------

class TestDagsterTemplateHeaderOptions:
    """AC4 — header comments document the options (existing convention).

    CLI-parseability of every ``# - `` line is enforced globally by the
    shared convention test (tests/shared/test_template_conventions.py).
    """

    def test_load_template_header_documents_known_options(self):
        path = (
            MODULE_RESOURCES["dagster"]
            / "templates" / "dags" / "load" / "dagster__scheduled_table__shell.py.j2"
        )
        options = parse_header_options(path)
        expected = {
            "sl_env_var", "SL_STARLAKE_PATH", "pre_load_strategy",
            "retries", "retry_delay",
        }
        missing = expected - set(options)
        assert not missing, f"Load template header no longer documents: {missing}"

    def test_transform_template_header_documents_known_options(self):
        path = (
            MODULE_RESOURCES["dagster"]
            / "templates" / "dags" / "transform" / "dagster__scheduled_task__shell.py.j2"
        )
        options = parse_header_options(path)
        expected = {
            "sl_env_var", "SL_STARLAKE_PATH", "run_dependencies_first",
            "retries", "retry_delay",
        }
        missing = expected - set(options)
        assert not missing, f"Transform template header no longer documents: {missing}"
