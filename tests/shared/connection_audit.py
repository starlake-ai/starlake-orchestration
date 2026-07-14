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
from typing import List, Sequence, Tuple

# Connection-configuration vocabulary that must never appear in an
# orchestrator module: resolving any of these is Starlake CLI's job.
FORBIDDEN_TEXT_PATTERNS: Tuple[str, ...] = (
    "connectionRef",
    "application.sl.yml",
    "jdbc:duckdb",
    "jdbc:postgresql",
    "jdbc:snowflake",
    "jdbc:redshift",
    "jdbc:mysql",
    "org.duckdb.DuckDBDriver",
    "org.postgresql.Driver",
    "net.snowflake.client.jdbc",
    "com.amazon.redshift",
)

# Warehouse/database CLIENT libraries.  Orchestrator platform SDKs are NOT
# listed: sqlalchemy (Airflow metadata DB), google.cloud.run_v2 (Cloud Run
# execution environment) and airflow.providers.google (Dataproc operators)
# are orchestrator/compute concerns, not data-warehouse connections.
# ai.starlake.odbc is the core's SQL-session executor (DuckDBSession,
# SnowflakeSession, ...) — importing it from an orchestrator module would
# smuggle database logic in indirectly, so it is forbidden here.  It must
# stay OUT of FORBIDDEN_TEXT_PATTERNS: the string appears in a list literal
# at starlake-snowflake/.../helper/__init__.py:36 (not an import).
FORBIDDEN_IMPORT_ROOTS: Tuple[str, ...] = (
    "duckdb",
    "psycopg2",
    "psycopg",
    "google.cloud.bigquery",
    "snowflake",           # allowlisted per-module (snowpark/core = platform SDK)
    "databricks",
    "redshift_connector",
    "pymysql",
    "mysql",
    "ai.starlake.odbc",
)


def _matches(name: str, roots: Sequence[str]) -> bool:
    return any(name == r or name.startswith(r + ".") for r in roots)


def _resolve_relative_base(py: Path, module_src: Path, level: int) -> str:
    """Resolve the base package of a relative import to a dotted name.

    ``module_src`` points at the scanned ``.../ai/starlake`` package
    directory, so the package of ``.../ai/starlake/airflow/bash/x.py``
    is ``ai.starlake.airflow.bash``.  Relative imports matter because
    the shared ``ai.starlake`` namespace makes ``from ..odbc import X``
    inside an orchestrator module resolve to ``ai.starlake.odbc`` —
    skipping them would leave a bypass in the audit fence.

    Returns ``""`` when *level* escapes the scanned tree (such an import
    would fail at runtime anyway).
    """
    pkg = ("ai", "starlake") + py.parent.relative_to(module_src).parts
    if level - 1 >= len(pkg):
        return ""
    base = pkg[: len(pkg) - (level - 1)] if level > 1 else pkg
    return ".".join(base)


def find_connection_logic_violations(
    module_src: Path,
    allowed_import_prefixes: Sequence[str] = (),
) -> Tuple[List[Path], List[str]]:
    """Scan an orchestrator module's source tree for database-specific logic.

    Returns ``(scanned_files, violations)``.  Excludes stale ``build/``
    artifacts and ``__pycache__``.  Callers must assert on BOTH values so
    the audit cannot silently pass by scanning zero files.

    ``from X import Y`` is checked as ``X.Y`` (not just ``X``) so the
    idiomatic ``from google.cloud import bigquery`` cannot slip past the
    ``google.cloud.bigquery`` root; relative imports are resolved against
    the file's package for the same reason (see _resolve_relative_base).
    """
    py_files = [
        p
        for p in sorted(module_src.rglob("*.py"))
        if "build" not in p.parts and "__pycache__" not in p.parts
    ]
    violations: List[str] = []
    for py in py_files:
        text = py.read_text()
        for pattern in FORBIDDEN_TEXT_PATTERNS:
            if pattern in text:
                violations.append(f"{py}: contains forbidden pattern '{pattern}'")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0:
                    base = node.module or ""
                else:
                    base = _resolve_relative_base(py, module_src, node.level)
                    if node.module:
                        base = f"{base}.{node.module}" if base else node.module
                if not base:
                    continue
                names = [base] + [f"{base}.{alias.name}" for alias in node.names]
            else:
                continue
            for name in names:
                if _matches(name, FORBIDDEN_IMPORT_ROOTS) and not _matches(
                    name, allowed_import_prefixes
                ):
                    violations.append(f"{py}: imports database client '{name}'")
    return py_files, violations
