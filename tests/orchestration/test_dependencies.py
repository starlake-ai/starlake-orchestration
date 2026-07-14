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
import json
from pathlib import Path

import pytest

from ai.starlake.job.starlake_job import (
    IStarlakeJob,
    StarlakeExecutionEnvironment,
    StarlakeExecutionMode,
    StarlakeJobFactory,
    StarlakeOrchestrator,
    TaskType,
)
from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy
from ai.starlake.dataset.starlake_dataset import (
    DatasetTriggeringStrategy,
    StarlakeDatasetType,
)
from ai.starlake.orchestration.starlake_orchestration import OrchestrationFactory
from ai.starlake.orchestration.starlake_dependencies import (
    StarlakeDependency,
    StarlakeDependencyType,
)
from ai.starlake.common import StarlakeCronPeriod

from tests.orchestration.conftest import StubJob, StubOrchestration, _STUB_MODULE_NAME


# ---------------------------------------------------------------------------
# 3.1  StarlakeOrchestrator enum completeness
# ---------------------------------------------------------------------------

class TestStarlakeOrchestratorEnum:
    def test_members(self):
        expected = {"AIRFLOW", "COMPOSER", "DAGSTER", "SNOWFLAKE", "STARLAKE"}
        assert set(StarlakeOrchestrator.__members__.keys()) == expected

    def test_is_str_subclass(self):
        assert issubclass(StarlakeOrchestrator, str)

    def test_values(self):
        assert StarlakeOrchestrator.AIRFLOW.value == "airflow"
        assert StarlakeOrchestrator.COMPOSER.value == "airflow"
        assert StarlakeOrchestrator.DAGSTER.value == "dagster"
        assert StarlakeOrchestrator.SNOWFLAKE.value == "snowflake"
        assert StarlakeOrchestrator.STARLAKE.value == "starlake"


# ---------------------------------------------------------------------------
# 3.2  StarlakeExecutionEnvironment enum completeness
# ---------------------------------------------------------------------------

class TestStarlakeExecutionEnvironmentEnum:
    def test_members(self):
        expected = {"CLOUD_RUN", "DATAPROC", "FARGATE", "SHELL", "SQL"}
        assert set(StarlakeExecutionEnvironment.__members__.keys()) == expected

    def test_is_str_subclass(self):
        assert issubclass(StarlakeExecutionEnvironment, str)


# ---------------------------------------------------------------------------
# 3.3  StarlakeExecutionMode enum completeness
# ---------------------------------------------------------------------------

class TestStarlakeExecutionModeEnum:
    def test_members(self):
        expected = {"DRY_RUN", "RUN", "BACKFILL"}
        assert set(StarlakeExecutionMode.__members__.keys()) == expected

    def test_is_str_subclass(self):
        assert issubclass(StarlakeExecutionMode, str)


# ---------------------------------------------------------------------------
# 3.4  TaskType enum completeness
# ---------------------------------------------------------------------------

class TestTaskTypeEnum:
    def test_members(self):
        expected = {"START", "PRELOAD", "IMPORT", "STAGE", "LOAD", "TRANSFORM", "EMPTY", "END"}
        assert set(TaskType.__members__.keys()) == expected

    def test_is_str_subclass(self):
        assert issubclass(TaskType, str)


# ---------------------------------------------------------------------------
# 3.5  TaskType.from_str()
# ---------------------------------------------------------------------------

class TestTaskTypeFromStr:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("start", TaskType.START),
            ("load", TaskType.LOAD),
            ("transform", TaskType.TRANSFORM),
            ("empty", TaskType.EMPTY),
            ("end", TaskType.END),
            ("preload", TaskType.PRELOAD),
            ("stage", TaskType.STAGE),
        ],
    )
    def test_valid_values(self, value, expected):
        assert TaskType.from_str(value) == expected

    def test_case_insensitive(self):
        assert TaskType.from_str("LOAD") == TaskType.LOAD

    def test_invalid_returns_none(self):
        assert TaskType.from_str("invalid_type") is None


# ---------------------------------------------------------------------------
# 3.6  StarlakePreLoadStrategy enum
# ---------------------------------------------------------------------------

class TestStarlakePreLoadStrategyEnum:
    def test_members(self):
        expected = {"IMPORTED", "ACK", "PENDING", "NONE"}
        assert set(StarlakePreLoadStrategy.__members__.keys()) == expected

    def test_is_valid(self):
        assert StarlakePreLoadStrategy.is_valid("imported")
        assert StarlakePreLoadStrategy.is_valid("ack")
        assert StarlakePreLoadStrategy.is_valid("pending")
        assert StarlakePreLoadStrategy.is_valid("none")
        assert not StarlakePreLoadStrategy.is_valid("invalid")

    def test_all_strategies(self):
        strategies = StarlakePreLoadStrategy.all_strategies()
        assert len(strategies) == 4


# ---------------------------------------------------------------------------
# 3.7  StarlakeDependencyType enum
# ---------------------------------------------------------------------------

class TestStarlakeDependencyTypeEnum:
    def test_members(self):
        expected = {"TASK", "TABLE"}
        assert set(StarlakeDependencyType.__members__.keys()) == expected

    def test_values(self):
        assert StarlakeDependencyType.TASK.value == "task"
        assert StarlakeDependencyType.TABLE.value == "table"


# ---------------------------------------------------------------------------
# 3.8  DatasetTriggeringStrategy enum
# ---------------------------------------------------------------------------

class TestDatasetTriggeringStrategyEnum:
    def test_members(self):
        expected = {"ALL", "ANY"}
        assert set(DatasetTriggeringStrategy.__members__.keys()) == expected

    def test_is_valid(self):
        assert DatasetTriggeringStrategy.is_valid("all")
        assert DatasetTriggeringStrategy.is_valid("any")
        assert not DatasetTriggeringStrategy.is_valid("invalid")


# ---------------------------------------------------------------------------
# 3.9  StarlakeDatasetType enum
# ---------------------------------------------------------------------------

class TestStarlakeDatasetTypeEnum:
    def test_members(self):
        expected = {"LOAD", "TRANSFORM"}
        assert set(StarlakeDatasetType.__members__.keys()) == expected

    def test_values(self):
        assert StarlakeDatasetType.LOAD.value == "load"
        assert StarlakeDatasetType.TRANSFORM.value == "transform"


# ---------------------------------------------------------------------------
# 3.10  StarlakeCronPeriod enum
# ---------------------------------------------------------------------------

class TestStarlakeCronPeriodEnum:
    def test_members(self):
        expected = {"DAY", "WEEK", "MONTH", "YEAR"}
        assert set(StarlakeCronPeriod.__members__.keys()) == expected

    def test_from_str_valid(self):
        assert StarlakeCronPeriod.from_str("day") == StarlakeCronPeriod.DAY
        assert StarlakeCronPeriod.from_str("WEEK") == StarlakeCronPeriod.WEEK

    def test_from_str_invalid(self):
        with pytest.raises(ValueError, match="Unsupported cron period"):
            StarlakeCronPeriod.from_str("hourly")


# ---------------------------------------------------------------------------
# 3.11  StarlakeJobFactory register/create
# ---------------------------------------------------------------------------

class TestStarlakeJobFactory:
    def test_register_and_create(self, monkeypatch):
        import copy
        monkeypatch.setattr(StarlakeJobFactory, "_registry", copy.deepcopy(StarlakeJobFactory._registry))
        monkeypatch.setattr(StarlakeJobFactory, "_initialized", True)
        StarlakeJobFactory.register_job(StubJob)
        assert "STUB" in StarlakeJobFactory._registry
        assert "SHELL" in StarlakeJobFactory._registry["STUB"]

        job = StarlakeJobFactory.create_job(
            filename="test_factory.py",
            module_name=_STUB_MODULE_NAME,
            orchestrator="STUB",
            execution_environment="SHELL",
            options={},
        )
        assert job is not None
        assert isinstance(job, StubJob)

    def test_create_unknown_raises(self, monkeypatch):
        monkeypatch.setattr(StarlakeJobFactory, "_registry", {})
        monkeypatch.setattr(StarlakeJobFactory, "_initialized", True)
        with pytest.raises(ValueError, match="not found in registry"):
            StarlakeJobFactory.create_job(
                filename="test.py",
                module_name=_STUB_MODULE_NAME,
                orchestrator="UNKNOWN",
                execution_environment="SHELL",
                options={},
            )


# ---------------------------------------------------------------------------
# 3.12  OrchestrationFactory register/create
# ---------------------------------------------------------------------------

class TestOrchestrationFactory:
    def test_register_and_create(self, monkeypatch, stub_job):
        import copy
        monkeypatch.setattr(OrchestrationFactory, "_registry", copy.deepcopy(OrchestrationFactory._registry))
        monkeypatch.setattr(OrchestrationFactory, "_initialized", True)
        OrchestrationFactory.register_orchestration(StubOrchestration)
        assert "STUB" in OrchestrationFactory._registry

        orch = OrchestrationFactory.create_orchestration(job=stub_job)
        assert orch is not None
        assert isinstance(orch, StubOrchestration)

    def test_create_unknown_raises(self, monkeypatch, stub_job):
        monkeypatch.setattr(OrchestrationFactory, "_registry", {})
        monkeypatch.setattr(OrchestrationFactory, "_initialized", True)
        with pytest.raises(ValueError, match="Unknown orchestrator type"):
            OrchestrationFactory.create_orchestration(job=stub_job)


# ---------------------------------------------------------------------------
# 3.13  Import isolation (NFR13)
# ---------------------------------------------------------------------------

class TestImportIsolation:
    def test_core_module_no_orchestrator_imports(self):
        """Core module exports must NOT import from orchestrator-specific modules."""
        core_src = Path(__file__).resolve().parents[2] / "starlake-orchestration" / "src" / "main" / "python" / "ai" / "starlake"
        assert core_src.exists(), f"Core source directory not found: {core_src}"
        orchestrator_prefixes = ("ai.starlake.airflow", "ai.starlake.dagster", "ai.starlake.snowflake")
        violations = []
        py_files_scanned = 0

        for py_file in core_src.rglob("*.py"):
            source = py_file.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(py_file))
            except SyntaxError:
                continue
            py_files_scanned += 1
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    if any(node.module.startswith(prefix) for prefix in orchestrator_prefixes):
                        violations.append(f"{py_file}: imports {node.module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if any(alias.name.startswith(prefix) for prefix in orchestrator_prefixes):
                            violations.append(f"{py_file}: imports {alias.name}")

        assert py_files_scanned > 0, f"No Python files found in {core_src} — test is not scanning anything"
        assert violations == [], (
            f"NFR13 violation — core module imports orchestrator modules:\n"
            + "\n".join(violations)
        )
