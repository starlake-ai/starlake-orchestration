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

import sys
import types
from typing import List, Optional, Union

import pytest

from ai.starlake.job.starlake_job import (
    IStarlakeJob,
    StarlakeExecutionMode,
    StarlakeJobFactory,
    TaskType,
)
from ai.starlake.job.spark_config import StarlakeSparkConfig
from ai.starlake.dataset.starlake_dataset import (
    AbstractEvent,
    DatasetTriggeringStrategy,
    StarlakeDataset,
)
from ai.starlake.orchestration.starlake_orchestration import (
    AbstractOrchestration,
    AbstractPipeline,
    AbstractTaskGroup,
    OrchestrationFactory,
    TaskGroupContext,
)
from ai.starlake.orchestration.starlake_schedules import StarlakeSchedule
from ai.starlake.orchestration.starlake_dependencies import StarlakeDependencies


# ---------------------------------------------------------------------------
# Fake caller module — IStarlakeJob.__init__ reads sys.modules[module_name]
# ---------------------------------------------------------------------------

_STUB_MODULE_NAME = "tests.orchestration._stub_caller"


@pytest.fixture(autouse=True, scope="session")
def _register_stub_module():
    """Inject a fake caller module into sys.modules for the test session."""
    _stub_mod = types.ModuleType(_STUB_MODULE_NAME)
    _stub_mod.__file__ = __file__
    sys.modules[_STUB_MODULE_NAME] = _stub_mod
    yield
    sys.modules.pop(_STUB_MODULE_NAME, None)


# ---------------------------------------------------------------------------
# Concrete stubs for abstract classes
# ---------------------------------------------------------------------------

class StubJob(IStarlakeJob):
    """Minimal concrete job for core unit tests."""

    @classmethod
    def sl_orchestrator(cls) -> str:
        return "STUB"

    @classmethod
    def sl_execution_environment(cls) -> str:
        return "SHELL"

    def sl_job(
        self,
        task_id,
        arguments,
        spark_config=None,
        dataset=None,
        task_type=TaskType.EMPTY,
        **kwargs,
    ):
        return {"task_id": task_id, "arguments": arguments, "task_type": task_type}

    def dummy_op(self, task_id, events=None, task_type=TaskType.EMPTY, **kwargs):
        return {"task_id": task_id, "events": events}

    def skip_or_start_op(self, task_id, upstream_task=None, **kwargs):
        return None

    @classmethod
    def to_event(cls, dataset, source=None):
        return {"dataset": dataset, "source": source}


class StubOrchestration(AbstractOrchestration):
    @classmethod
    def sl_orchestrator(cls) -> str:
        return "STUB"

    def sl_create_pipeline(
        self,
        schedule=None,
        dependencies=None,
        **kwargs,
    ):
        return StubPipeline(
            job=self.job,
            orchestration_cls=self,
            schedule=schedule,
            dependencies=dependencies,
            dag=None,
            orchestration=self,
        )

    def sl_create_task_group(self, group_id, pipeline, **kwargs):
        return AbstractTaskGroup(group_id, orchestration_cls=self)


class StubPipeline(AbstractPipeline):
    def run(
        self,
        logical_date=None,
        timeout="120",
        mode=StarlakeExecutionMode.RUN,
        **kwargs,
    ):
        pass  # no-op for unit tests


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def stub_job():
    """Create a StubJob with sensible defaults."""
    return StubJob(
        filename="test_pipeline.py",
        module_name=_STUB_MODULE_NAME,
        options={},
    )


@pytest.fixture
def stub_orchestration(stub_job):
    """Create a StubOrchestration wrapping the stub job."""
    return StubOrchestration(job=stub_job)


@pytest.fixture
def stub_schedule():
    """Create a minimal StarlakeSchedule."""
    from ai.starlake.orchestration.starlake_schedules import StarlakeDomain, StarlakeTable
    return StarlakeSchedule(
        name="daily",
        cron="0 0 * * *",
        domains=[
            StarlakeDomain(
                name="test_domain",
                final_name="test_domain",
                tables=[StarlakeTable(name="table1")],
            )
        ],
    )


# _clean_context_stack is provided by tests/conftest.py (root) as an
# autouse fixture — no need to duplicate it here.
