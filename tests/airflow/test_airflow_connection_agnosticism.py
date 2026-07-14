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

from pathlib import Path

from tests.shared.connection_audit import find_connection_logic_violations

_MODULE_SRC = (
    Path(__file__).resolve().parents[2]
    / "starlake-airflow" / "src" / "main" / "python" / "ai" / "starlake"
)


class TestAirflowConnectionAgnosticism:
    """AC #4: no database-specific logic in the Airflow module.

    Legitimate DB-flavoured vocabulary deliberately NOT flagged:
    ``spark.datasource.bigquery.*`` Spark properties (Dataproc
    execution-environment tuning) and ``sqlalchemy`` imports targeting
    Airflow's own metadata DB — neither is warehouse connection config.
    """

    def test_no_database_specific_logic(self):
        assert _MODULE_SRC.is_dir(), f"Module source not found at {_MODULE_SRC}"
        py_files, violations = find_connection_logic_violations(_MODULE_SRC)
        # Sentinel guard: the scan must have covered the real module,
        # not an empty directory (see feedback_test_assertion_quality).
        assert any(p.name == "starlake_airflow_job.py" for p in py_files), (
            f"Scan missed the module entry point; scanned: {py_files}"
        )
        assert violations == [], "\n".join(violations)
