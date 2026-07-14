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

import shutil
import subprocess
from pathlib import Path
from typing import Mapping, Tuple

import pytest

from tests.shared.conftest import _env_var
from tests.shared.lineage_utils import (
    EXPECTED_TRANSFORM_EDGES,
    EXPECTED_TRANSFORM_NODES,
    extract_embedded_lineage,
    is_transform_dag,
    normalize_lineage,
)

pytestmark = [
    pytest.mark.integration,
]

# String literals on purpose: importing StarlakeOrchestrator here would violate
# NFR13 (tests/shared/ must stay orchestrator-agnostic — Story 1.1 convention).
# dag-generate only renders templates bundled in the CLI assembly; no Python
# orchestrator package is imported because this test never load_pipelines().
_LOAD_DAG_REF = "airflow_load_shell"
_TRANSFORM_DAG_REF = "airflow_transform_shell"


def _dag_generate(
    cli: str,
    version: str,
    starlake_env: Mapping[str, str],
    sample_project_path: Path,
    tmp_path: Path,
    label: str,
) -> Path:
    """Copy the sample project and run dag-generate with the given CLI/version."""
    project = tmp_path / f"project-{label}"
    shutil.copytree(sample_project_path, project)
    out = tmp_path / f"dags-{label}"
    env = dict(starlake_env)
    env["SL_ROOT"] = str(project)
    # starlake.sh picks the jar via SL_JAR_NAME=...-$SL_VERSION-assembly.jar:
    # each CLI install MUST run with its own version, never the inherited one.
    env["SL_VERSION"] = version
    env["LOAD_DAG_REF"] = _LOAD_DAG_REF
    env["TRANSFORM_DAG_REF"] = _TRANSFORM_DAG_REF
    try:
        result = subprocess.run(
            [cli, "dag-generate", "--outputDir", str(out)],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"dag-generate ({label}, SL_VERSION={version}) timed out after 300s")
    assert result.returncode == 0, (
        f"dag-generate ({label}, SL_VERSION={version}) failed:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return out


def _normalized(out_dir: Path) -> Tuple[dict, list]:
    files = sorted(out_dir.glob("*.py"))
    assert files, f"No .py files generated in {out_dir}"
    transform_files = [f for f in files if is_transform_dag(f)]
    assert len(transform_files) == 1
    norm = normalize_lineage(extract_embedded_lineage(transform_files[0]))
    return norm, [f.name for f in files]


class TestLineageCliVersionTolerance:
    """NFR7: the lineage the framework consumes must be stable across CLI versions."""

    def test_lineage_identical_across_cli_versions(
        self,
        starlake_cli,
        starlake_cli_secondary,
        starlake_env,
        sample_project_path,
        tmp_path,
    ):
        primary_out = _dag_generate(
            starlake_cli, _env_var("SL_VERSION"),
            starlake_env, sample_project_path, tmp_path, "primary",
        )
        secondary_out = _dag_generate(
            starlake_cli_secondary, _env_var("SL_VERSION_SECONDARY"),
            starlake_env, sample_project_path, tmp_path, "secondary",
        )

        primary_norm, primary_files = _normalized(primary_out)
        secondary_norm, secondary_files = _normalized(secondary_out)

        # Both CLI versions must produce the SAME canonical lineage...
        # (technically implied by the two golden asserts below — kept because
        # its failure message shows the actual divergence side by side)
        assert primary_norm == secondary_norm, (
            f"Lineage diverged between CLI versions:\n"
            f"primary={primary_norm}\nsecondary={secondary_norm}"
        )
        # ...which is also the golden structure every orchestrator asserts.
        assert primary_norm["nodes"] == EXPECTED_TRANSFORM_NODES
        assert primary_norm["edges"] == EXPECTED_TRANSFORM_EDGES
        # Same generated file set (names come from the DAG config, not the CLI).
        assert primary_files == secondary_files
