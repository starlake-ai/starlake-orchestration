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

"""Shared utilities for Jinja2 template tests across the four modules.

Plain importable module (like ``set_env``/``get_duckdb`` in
``tests/shared/conftest.py``) — deliberately NOT fixtures, so both the
per-orchestrator template tests and the shared convention scan can use
them without conftest plumbing.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

MODULE_RESOURCES: Dict[str, Path] = {
    "airflow": PROJECT_ROOT / "starlake-airflow" / "src" / "main" / "resources",
    "dagster": PROJECT_ROOT / "starlake-dagster" / "src" / "main" / "resources",
    "snowflake": PROJECT_ROOT / "starlake-snowflake" / "src" / "main" / "resources",
    "orchestration": PROJECT_ROOT / "starlake-orchestration" / "src" / "main" / "resources",
}

# Orchestrator prefix (template filename) -> snippet include path
ORCHESTRATOR_SNIPPETS: Dict[str, str] = {
    "airflow": "templates/dags/__starlake_airflow_orchestrator.py.j2",
    "dagster": "templates/dags/__starlake_dagster_orchestrator.py.j2",
    "snowflake": "templates/dags/__starlake_snowflake_orchestrator.py.j2",
    "starlake": "templates/dags/__starlake_sl_orchestrator.py.j2",
}

# Orchestrator prefix -> StarlakeOrchestrator enum member set by the snippet.
# NOTE: the enum also defines COMPOSER = "airflow" (a value-alias of AIRFLOW
# that no snippet sets) — expectations are pinned here, never derived by
# iterating the enum.
ORCHESTRATOR_ENUMS: Dict[str, str] = {
    "airflow": "AIRFLOW",
    "dagster": "DAGSTER",
    "snowflake": "SNOWFLAKE",
    "starlake": "STARLAKE",
}

TEMPLATE_NAME_RE = re.compile(
    r"^(airflow|dagster|snowflake|starlake)__scheduled_(table|task)__[a-z0-9_]+\.py\.j2$"
)

# Header option line, e.g.:
# "# - ack_wait_timeout(3600): when ... the timeout in seconds [OPTIONAL]"
# "# - warehouse(COMPUTE_WH): the warehouse to use for the DAG [OPTIONAL], default to COMPUTE_WH"
#
# MUST mirror the Starlake CLI's own header parser — DagTemplateOption.fromLine
# (starlake/src/main/scala/ai/starlake/utils/AnyTemplateLoader.scala:39-62):
#  - line starts with "# - ";
#  - split(':') must yield EXACTLY 2 parts, i.e. exactly one ':' on the line
#    (a colon in the description makes the option INVISIBLE to the CLI);
#  - tag detected via contains("[OPTIONAL]"/"[REQUIRED]") — trailing text after
#    the tag is legal (the snowflake headers use ", default to ...").
# Verified 2026-07-14: every `# - ` line in all 20 user-facing templates
# matches this regex (exactly one colon, tag present).
HEADER_OPTION_RE = re.compile(
    r"^# - (?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"(\((?P<default>[^)]*)\))?"
    r"\s*:[^:]*\[(?P<tag>OPTIONAL|REQUIRED)\][^:]*$"
)


def make_jinja_env(module: str) -> Environment:
    """Jinja2 environment for *module*, with the core module as fallback root.

    Mirrors what the Starlake CLI does: templates include files from both the
    orchestrator jar and the starlake-orchestration jar.
    """
    roots = [str(MODULE_RESOURCES[module])]
    if module != "orchestration":
        roots.append(str(MODULE_RESOURCES["orchestration"]))
    return Environment(loader=FileSystemLoader(roots), keep_trailing_newline=True)


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
    def __init__(self, comment, template, options):
        self.comment = comment
        self.template = template
        self.options = options


_DEFAULT_OPTIONS = {
    "airflow": [
        _Option("SL_ROOT", "/tmp/test"),
        _Option("SL_ENV", "DUCKDB"),
        _Option("SL_STARLAKE_PATH", "starlake"),
        _Option("pre_load_strategy", "imported"),
        _Option("tags", "starbake"),
    ],
    "dagster": [
        _Option("SL_ROOT", "/tmp/test"),
        _Option("SL_ENV", "DUCKDB"),
        _Option("SL_STARLAKE_PATH", "starlake"),
        _Option("pre_load_strategy", "imported"),
        _Option("tags", "starbake"),
    ],
    "snowflake": [
        _Option("SL_ROOT", "/tmp/test"),
        _Option("SL_ENV", "DUCKDB"),
        _Option("stage_location", "staging"),
        _Option("warehouse", "COMPUTE_WH"),
        _Option("timezone", "UTC"),
    ],
}


class _Context:
    def __init__(self, orchestrator, template, comment):
        # .get with airflow fallback: "starlake"-prefixed templates have no
        # dedicated option set (no render-chain tests for them in this story).
        self.config = _Config(
            comment,
            template,
            list(_DEFAULT_OPTIONS.get(orchestrator, _DEFAULT_OPTIONS["airflow"])),
        )
        self.cron = "0 0 * * *"
        self.schedules = [
            _Schedule(
                schedule="daily",
                cron="0 0 * * *",
                domains={"starbake": ["customers", "orders", "products"]},
            ),
        ]
        self.dependencies = "[]"
        # Airflow snippet reads this (context.sl_airflow_access_control);
        # harmless superset for the other orchestrators.
        self.sl_airflow_access_control = "None"
        # Snowflake SQL execution snippet reads these; harmless superset.
        self.statements = "{}"
        self.expectationItems = "{}"
        self.audit = "{}"
        self.expectations = "{}"
        self.acl = "{}"


def make_mock_context(
    orchestrator: str,
    template: Optional[str] = None,
    comment: str = "Test DAG for loading starbake tables",
) -> object:
    """Superset mock of the context ``starlake dag-generate`` provides."""
    if template is None:
        template = f"load/{orchestrator}__scheduled_table__shell.py.j2"
    return _Context(orchestrator, template, comment)


def user_facing_templates() -> List[Path]:
    """All non-underscore *.py.j2 DAG templates across the four modules.

    "User-facing" per the Starlake CLI's own filter
    (``AnyTemplateLoader``: ``!name.startsWith("_") && name.endsWith(".j2")``):
    underscore-prefixed files are snippets.
    """
    files = []
    for resources in MODULE_RESOURCES.values():
        for sub in ("load", "transform"):
            folder = resources / "templates" / "dags" / sub
            if folder.is_dir():
                files.extend(
                    p for p in sorted(folder.glob("*.py.j2"))
                    if not p.name.startswith("_")
                )
    # Zero-scan guard (feedback_test_assertion_quality): 20 verified 2026-07-14
    # (airflow 8, dagster 8, snowflake 2, orchestration 2).
    assert len(files) >= 20, f"Expected >= 20 user-facing templates, found {len(files)}"
    return files


def parse_header_options(template_path: Path) -> Dict[str, str]:
    """Parse the leading comment block of a template into {option: tag}.

    Returns an empty dict if the file does not start with a header block.
    """
    options = {}
    for line in template_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("#"):
            break  # header block ends at the first non-comment line
        m = HEADER_OPTION_RE.match(line)
        if m:
            options[m.group("name")] = m.group("tag")
    return options


def assert_dag_generate_idempotent(runtime_dags, tmp_path_factory) -> None:
    """NFR2 at the CLI layer: a second dag-generate run is byte-identical.

    ``runtime_dags`` is the shared module-scoped fixture (tests/shared/conftest.py)
    that already ran dag-generate once; re-run with the SAME env into a fresh dir.
    """
    first_out, _isolated, env = runtime_dags
    # runtime_env prepended the CLI dir to env["PATH"]; subprocess resolves
    # the program via os.get_exec_path(env), so plain "starlake" hits the
    # same binary runtime_dags used.
    second_out = tmp_path_factory.mktemp("runtime_dags_rerun")
    result = subprocess.run(
        ["starlake", "dag-generate", "--outputDir", str(second_out)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"second dag-generate failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    first_files = sorted(p.name for p in first_out.glob("*.py"))
    second_files = sorted(p.name for p in second_out.glob("*.py"))
    assert first_files == second_files, (
        f"File sets differ: {first_files} vs {second_files}"
    )
    assert len(first_files) > 0, "dag-generate produced no files"
    for name in first_files:
        a = (first_out / name).read_bytes()
        b = (second_out / name).read_bytes()
        assert a == b, f"{name} is not byte-identical across dag-generate runs (NFR2)"
