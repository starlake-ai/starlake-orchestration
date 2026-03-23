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

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestSlImport(BaseTestOrchestration):
    """Abstract base for sl_import() shared functional tests."""

    # ------------------------------------------------------------------
    # Import single domain
    # ------------------------------------------------------------------

    def test_import_single_domain(self):
        """Import starbake domain, verify task args include
        ['stage', '--domains', 'starbake']."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_import(
            task_id="import_starbake",
            domain="starbake",
        )
        assert task is not None
        args = self.get_task_arguments(task)
        assert "stage" in args
        assert self.get_arg_value(args, "--domains") == "starbake"

    # ------------------------------------------------------------------
    # Import with tables filter
    # ------------------------------------------------------------------

    def test_import_with_tables_filter(self):
        """Import specific tables, verify --tables arg is present."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_import(
            task_id="import_starbake_customers",
            domain="starbake",
            tables=["customers", "orders"],
        )
        assert task is not None
        args = self.get_task_arguments(task)
        tables_value = self.get_arg_value(args, "--tables")
        # Tables are joined with comma in sl_import
        for table in ("customers", "orders"):
            assert table in tables_value

    # ------------------------------------------------------------------
    # Import task ID format
    # ------------------------------------------------------------------

    def test_import_task_id_format(self):
        """Verify default task ID follows 'import_{domain}' convention
        when task_id is not provided (empty string)."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        # Empty task_id triggers the default: "import_{domain}"
        task = job.sl_import(
            task_id="",
            domain="starbake",
        )
        assert task is not None
        actual_id = self.get_task_id(task)
        assert actual_id == "import_starbake"
