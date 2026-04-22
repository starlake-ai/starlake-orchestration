import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent

# Make `ai.starlake.*` importable from each subproject's main/python tree.
# At repo root so it applies to tests in every subproject test tree.
for subproject in ("starlake-orchestration", "starlake-airflow", "starlake-dagster"):
    main_python = _REPO_ROOT / subproject / "src" / "main" / "python"
    if main_python.exists():
        sys.path.insert(0, str(main_python))
