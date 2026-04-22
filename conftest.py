"""Pytest root conftest — stitches the three subproject `src/main/python/` trees
into a single `ai.starlake.*` namespace for test runs.

In production, these subprojects are installed as separate wheels that share
the `ai.starlake` namespace via setup-time configuration. In development, each
subproject has its own `ai/__init__.py` + `ai/starlake/__init__.py` (license
header only), which means a naive `sys.path` append would let the first
subproject on the path win — shadowing the others. We extend `__path__` on
both package objects so sub-imports can find siblings across all subprojects.
"""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent

_SUBPROJECTS = ("starlake-orchestration", "starlake-airflow", "starlake-dagster")

for subproject in _SUBPROJECTS:
    main_python = _REPO_ROOT / subproject / "src" / "main" / "python"
    if main_python.exists():
        sys.path.insert(0, str(main_python))

import ai  # noqa: E402
import ai.starlake  # noqa: E402

for subproject in _SUBPROJECTS:
    main_python = _REPO_ROOT / subproject / "src" / "main" / "python"
    if not main_python.exists():
        continue
    ai_dir = str(main_python / "ai")
    ai_starlake_dir = str(main_python / "ai" / "starlake")
    if ai_dir not in ai.__path__:
        ai.__path__.append(ai_dir)
    if ai_starlake_dir not in ai.starlake.__path__:
        ai.starlake.__path__.append(ai_starlake_dir)
