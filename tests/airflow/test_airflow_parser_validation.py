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

"""AC3 / NFR8 — generated DAGs are valid for Airflow's parser (2.x AND 3.x).

Full chain: ``starlake dag-generate`` (shared ``runtime_dags`` fixture)
→ generated ``.py`` files → Airflow's filesystem ``DagBag``.

Filesystem DagBag parsing needs NO metadata DB, bundle config, or
``DAG.bulk_write_to_db`` — none of the runtime-suite Airflow 3 gotchas
apply, so this module must NOT carry any 2.x-only skip: it runs
un-skipped on BOTH CI legs (Product Decision 3 of the story).
"""

from __future__ import annotations

import pytest

from tests.shared.conftest import restore_env, set_env
from tests.shared.template_test_utils import assert_dag_generate_idempotent

try:
    import airflow

    AIRFLOW_AVAILABLE = True
    AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
    if AIRFLOW_VERSION >= (3, 0):
        # airflow.models.dagbag.DagBag is a DeprecatedImportWarning shim on 3.x;
        # the filesystem DagBag moved to airflow.dag_processing.dagbag.
        from airflow.dag_processing.dagbag import DagBag
    else:
        from airflow.models.dagbag import DagBag
except ImportError:
    AIRFLOW_AVAILABLE = False
    AIRFLOW_VERSION = (0, 0)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Requires Apache Airflow"),
]


@pytest.fixture(scope="module")
def dagbag(runtime_dags, airflow_home):
    """Parse the dag-generate output directory through Airflow's DagBag.

    Env vars from runtime_dags (SL_ROOT, LOAD_DAG_REF, ...) must be live while
    DagBag imports the generated modules, exactly as for load_pipelines().
    ``safe_mode=False`` because the generated files do not necessarily contain
    the literal heuristic strings DagBag greps for before importing.
    """
    dags_dir, _isolated, env = runtime_dags
    original = set_env(env)
    try:
        if AIRFLOW_VERSION >= (3, 0):
            bag = DagBag(dag_folder=str(dags_dir), safe_mode=False, collect_dags=True)
        else:
            bag = DagBag(
                dag_folder=str(dags_dir), include_examples=False, safe_mode=False
            )
    finally:
        restore_env(original)
    return bag


class TestAirflowDagBagValidation:
    """AC3 / NFR8 — generated DAGs are valid for Airflow's parser (2.x AND 3.x)."""

    def test_dagbag_has_no_import_errors(self, dagbag):
        assert dagbag.import_errors == {}, (
            f"DagBag import errors: {dagbag.import_errors}"
        )

    def test_dagbag_collected_generated_dags(self, dagbag, runtime_dags):
        dags_dir, _, _ = runtime_dags
        generated_files = sorted(dags_dir.glob("*.py"))
        assert len(generated_files) > 0
        assert len(dagbag.dags) >= 1, (
            f"DagBag collected no DAGs from {len(generated_files)} generated files"
        )

    def test_dagbag_dags_have_tasks(self, dagbag):
        for dag_id, dag in dagbag.dags.items():
            assert len(dag.tasks) > 0, f"DAG {dag_id} parsed with zero tasks"


class TestAirflowDagGenerateIdempotence:
    """AC2 / NFR2 — dag-generate output is byte-identical across runs."""

    def test_dag_generate_is_idempotent(self, runtime_dags, tmp_path_factory):
        assert_dag_generate_idempotent(runtime_dags, tmp_path_factory)
