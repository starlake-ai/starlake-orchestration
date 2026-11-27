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

"""Every load template hands the table's freshness to its StarlakeTable (issue #166).

Starlake core gives each load template the domains and tables of a schedule in
``rawDomains``, as maps whose absent options are null. The templates read the
table's ``metadata.freshness.error`` and, when the table declares no freshness,
the domain's (core replaces the whole freshness object when it merges the
metadata). The rendered ``schedules`` block is evaluated with the core classes,
so the test needs no orchestrator.
"""

from __future__ import annotations

import re

import pytest

from ai.starlake.orchestration.starlake_schedules import StarlakeDomain, StarlakeSchedule, StarlakeTable

from tests.shared.template_test_utils import make_jinja_env, make_mock_context, user_facing_templates

LOAD_TEMPLATES = [path for path in user_facing_templates() if path.parent.name == "load"]

MODULES = {"airflow": "airflow", "dagster": "dagster", "snowflake": "snowflake", "starlake": "orchestration"}


def _freshness(error=None, warn=None):
    return {"warn": warn, "error": error}


def _raw_table(name, freshness=None):
    return {"name": name, "metadata": None if freshness is None else {"freshness": freshness}}


def _raw_domain(tables, freshness=None):
    return {
        "name": "starbake",
        "metadata": None if freshness is None else {"freshness": freshness},
        "tables": tables,
    }


def _render_schedules(template_path, raw_domains="absent"):
    orchestrator = template_path.name.split("__")[0]
    template_name = f"load/{template_path.name}"
    context = make_mock_context(orchestrator, template=template_name)
    if raw_domains != "absent":
        context.schedules[0].rawDomains = raw_domains
    rendered = make_jinja_env(MODULES[orchestrator]).get_template(f"templates/dags/{template_name}").render(
        context=context, pyjson="{}"
    )
    block = re.search(r"^schedules\s*=\s*(\[.*?^\])", rendered, re.S | re.M)
    assert block, f"no schedules block in {template_path.name}"
    schedules = eval(block.group(1), {
        "StarlakeSchedule": StarlakeSchedule,
        "StarlakeDomain": StarlakeDomain,
        "StarlakeTable": StarlakeTable,
    })
    return {table.name: table.freshness for table in schedules[0].domains[0].tables}


def test_every_load_template_is_scanned():
    # airflow 4, dagster 4, starlake 1, snowflake 1
    assert len(LOAD_TEMPLATES) == 10


@pytest.mark.parametrize("template_path", LOAD_TEMPLATES, ids=lambda path: path.name)
class TestLoadTemplateFreshness:
    def test_the_table_freshness_wins_over_the_domain(self, template_path):
        freshness = _render_schedules(template_path, [_raw_domain(
            [_raw_table("customers", _freshness("2 days")), _raw_table("orders"), _raw_table("products")],
            freshness=_freshness("1 day"),
        )])
        assert freshness == {"customers": 172800, "orders": 86400, "products": 86400}

    def test_a_table_freshness_without_error_does_not_inherit(self, template_path):
        # core replaces the whole freshness object: a table declaring a warn
        # duration only publishes 0, not its domain's error
        freshness = _render_schedules(template_path, [_raw_domain(
            [_raw_table("customers", _freshness(warn="1 hour")), _raw_table("orders"), _raw_table("products")],
            freshness=_freshness("1 day"),
        )])
        assert freshness == {"customers": 0, "orders": 86400, "products": 86400}

    def test_no_freshness_declared(self, template_path):
        freshness = _render_schedules(template_path, [_raw_domain(
            [_raw_table("customers"), _raw_table("orders"), _raw_table("products")],
        )])
        assert freshness == {"customers": 0, "orders": 0, "products": 0}

    def test_tables_missing_from_raw_domains_are_still_loaded(self, template_path):
        freshness = _render_schedules(template_path, [_raw_domain([_raw_table("customers", _freshness("2 days"))])])
        assert freshness == {"customers": 172800, "orders": 0, "products": 0}

    @pytest.mark.parametrize("raw_domains", ["absent", None, []], ids=["absent", "null", "empty"])
    def test_without_raw_domains_every_table_is_loaded(self, template_path, raw_domains):
        # an older CLI: the template renders the same tables as before
        assert _render_schedules(template_path, raw_domains) == {"customers": 0, "orders": 0, "products": 0}
