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

"""AC3 / NFR8 — generated modules load as Dagster Definitions.

Full chain: ``starlake dag-generate`` (shared ``runtime_dags`` fixture)
→ generated ``.py`` files → ``load_pipelines()`` → module-bound ``defs``
→ ``Definitions.validate_loadable`` (the same resolution Dagster performs
when loading a code location).
"""

from __future__ import annotations

import sys

import pytest

from tests.shared.conftest import restore_env, set_env
from tests.shared.template_test_utils import assert_dag_generate_idempotent

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def loaded_definitions(runtime_dags):
    """load_pipelines() each generated file and collect the module-bound defs.

    DagsterOrchestration.__exit__ binds Definitions to the caller module:
    ``setattr(module, 'defs', defs)`` (starlake_dagster_orchestration.py:196),
    and load_pipelines registers the module as sys.modules[<file stem>]
    (ai/starlake/orchestration/__main__.py:26-30).
    """
    from ai.starlake.orchestration.__main__ import load_pipelines

    dags_dir, _isolated, env = runtime_dags
    original = set_env(env)
    try:
        all_defs = {}
        for py_file in sorted(dags_dir.glob("*.py")):
            pipelines = load_pipelines(str(py_file))
            assert pipelines, f"No pipelines loaded from {py_file.name}"
            module = sys.modules[py_file.stem]
            assert hasattr(module, "defs"), (
                f"{py_file.name} has no module-level 'defs' — repository "
                f"loading contract broken"
            )
            all_defs[py_file.name] = module.defs
    finally:
        restore_env(original)
    assert len(all_defs) > 0
    return all_defs


class TestDagsterRepositoryLoading:
    """AC3 / NFR8 — generated modules load as Dagster Definitions."""

    def test_defs_are_definitions_instances(self, loaded_definitions):
        from dagster import Definitions

        for name, defs in loaded_definitions.items():
            assert isinstance(defs, Definitions), f"{name}: {type(defs)}"

    def test_definitions_are_loadable(self, loaded_definitions):
        from dagster import Definitions

        for name, defs in loaded_definitions.items():
            # Same resolution Dagster performs when loading a code location.
            Definitions.validate_loadable(defs)

    def test_definitions_expose_jobs(self, loaded_definitions):
        for name, defs in loaded_definitions.items():
            # Dagster auto-adds an IMPLICIT asset job ("__ASSET_JOB", see
            # dagster._core.definitions.asset_job.IMPLICIT_ASSET_JOB_NAME)
            # to Definitions that carry asset defs (the transform pipeline's
            # upstream datasets) — it legitimately has zero ops. Only the
            # generated PIPELINE jobs must have ops.
            pipeline_jobs = [
                job for job in defs.get_all_job_defs()
                if not job.name.startswith("__ASSET_JOB")
            ]
            assert len(pipeline_jobs) > 0, f"{name} exposes no pipeline jobs"
            for job in pipeline_jobs:
                assert len(job.nodes) > 0, f"{name}:{job.name} has no ops"


class TestDagsterDagGenerateIdempotence:
    """AC2 / NFR2 — dag-generate output is byte-identical across runs."""

    def test_dag_generate_is_idempotent(self, runtime_dags, tmp_path_factory):
        assert_dag_generate_idempotent(runtime_dags, tmp_path_factory)
