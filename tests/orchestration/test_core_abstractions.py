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

import pytest

from ai.starlake.job.starlake_job import IStarlakeJob, TaskType
from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy
from ai.starlake.dataset.starlake_dataset import AbstractEvent, DatasetTriggeringStrategy
from ai.starlake.orchestration.starlake_orchestration import (
    AbstractOrchestration,
    AbstractPipeline,
    AbstractTask,
    AbstractTaskGroup,
    TaskGroupContext,
)
from ai.starlake.orchestration.starlake_schedules import StarlakeSchedule

from tests.orchestration.conftest import (
    StubJob,
    StubOrchestration,
    StubPipeline,
    _STUB_MODULE_NAME,
)


# ---------------------------------------------------------------------------
# 2.1  IStarlakeJob contract — abstract methods prevent direct instantiation
# ---------------------------------------------------------------------------

class TestIStarlakeJobContract:
    def test_abstract_methods_defined(self):
        """IStarlakeJob declares sl_job, dummy_op, skip_or_start_op as @abstractmethod."""
        import inspect
        abstract_methods = set()
        for name, method in inspect.getmembers(IStarlakeJob):
            if getattr(method, "__isabstractmethod__", False):
                abstract_methods.add(name)
        assert "sl_job" in abstract_methods
        assert "dummy_op" in abstract_methods
        assert "skip_or_start_op" in abstract_methods


# ---------------------------------------------------------------------------
# 2.2  IStarlakeJob properties
# ---------------------------------------------------------------------------

class TestIStarlakeJobProperties:
    def test_pre_load_strategy_default(self, stub_job):
        assert stub_job.pre_load_strategy == StarlakePreLoadStrategy.NONE

    def test_sl_env_vars_type(self, stub_job):
        assert isinstance(stub_job.sl_env_vars, dict)

    def test_pipeline_id(self, stub_job):
        assert stub_job.pipeline_id == "TEST_PIPELINE"

    def test_dataset_triggering_strategy_default(self, stub_job):
        assert stub_job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY

    def test_events_initially_empty(self, stub_job):
        assert stub_job.events == []


# ---------------------------------------------------------------------------
# 2.3  IStarlakeJob.sl_env() is @final and returns env dict
# ---------------------------------------------------------------------------

class TestIStarlakeJobSlEnv:
    def test_sl_env_no_args_returns_none(self, stub_job):
        """sl_env() with no args calls env.update() which returns None."""
        env = stub_job.sl_env()
        assert env is None

    def test_sl_env_with_args_returns_dict(self, stub_job):
        """sl_env() with args returns a dict of environment variables."""
        env_with_args = stub_job.sl_env(["load", "--options", "KEY=VAL"])
        assert isinstance(env_with_args, dict)

    def test_sl_env_exists_on_interface(self):
        """sl_env is defined on IStarlakeJob (decorated @final — static analysis only, not runtime-enforceable)."""
        assert hasattr(IStarlakeJob, "sl_env")
        assert callable(getattr(IStarlakeJob, "sl_env"))


# ---------------------------------------------------------------------------
# 2.4  IStarlakeJob.sl_load/sl_transform/sl_import/sl_pre_load delegate to sl_job
# ---------------------------------------------------------------------------

class TestIStarlakeJobDelegation:
    def test_sl_load_delegates(self, stub_job, stub_orchestration, stub_schedule):
        """sl_load() should call sl_job() and return its result."""
        with stub_orchestration:
            pipeline = stub_orchestration.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                result = stub_job.sl_load(
                    task_id="load_test",
                    domain="test_domain",
                    table="table1",
                )
                assert result is not None
                assert result["task_id"] == "load_test"
                assert result["task_type"] == TaskType.LOAD

    def test_sl_transform_delegates(self, stub_job, stub_orchestration, stub_schedule):
        with stub_orchestration:
            pipeline = stub_orchestration.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                result = stub_job.sl_transform(
                    task_id="transform_test",
                    transform_name="my_transform",
                )
                assert result is not None
                assert result["task_id"] == "transform_test"
                assert result["task_type"] == TaskType.TRANSFORM

    def test_sl_import_delegates(self, stub_job, stub_orchestration, stub_schedule):
        with stub_orchestration:
            pipeline = stub_orchestration.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                result = stub_job.sl_import(
                    task_id="import_test",
                    domain="test_domain",
                )
                assert result is not None
                assert result["task_type"] == TaskType.STAGE

    def test_sl_pre_load_returns_none_for_none_strategy(self, stub_job):
        """With NONE strategy, sl_pre_load returns None."""
        result = stub_job.sl_pre_load(domain="test_domain")
        assert result is None


# ---------------------------------------------------------------------------
# 2.5  AbstractOrchestration contract
# ---------------------------------------------------------------------------

class TestAbstractOrchestrationContract:
    def test_sl_create_pipeline_is_abstract(self):
        """sl_create_pipeline is declared as @abstractmethod."""
        method = getattr(AbstractOrchestration, "sl_create_pipeline", None)
        assert method is not None
        assert getattr(method, "__isabstractmethod__", False)

    def test_sl_create_task_group_is_abstract(self):
        """sl_create_task_group is declared as @abstractmethod."""
        method = getattr(AbstractOrchestration, "sl_create_task_group", None)
        assert method is not None
        assert getattr(method, "__isabstractmethod__", False)


# ---------------------------------------------------------------------------
# 2.6  AbstractOrchestration.pipelines management
# ---------------------------------------------------------------------------

class TestAbstractOrchestrationPipelines:
    def test_pipelines_empty_on_enter(self, stub_orchestration, stub_schedule):
        with stub_orchestration as orch:
            assert orch.pipelines == []

    def test_pipelines_populated_after_pipeline_creation(
        self, stub_orchestration, stub_schedule
    ):
        with stub_orchestration as orch:
            pipeline = orch.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                pass  # __exit__ registers pipeline
            assert len(orch.pipelines) == 1
            assert orch.pipelines[0] is pipeline


# ---------------------------------------------------------------------------
# 2.7  AbstractPipeline requires schedule or dependencies
# ---------------------------------------------------------------------------

class TestAbstractPipelineValidation:
    def test_raises_without_schedule_or_dependencies(self, stub_job, stub_orchestration):
        with pytest.raises(ValueError, match="Either a schedule or dependencies must be provided"):
            StubPipeline(
                job=stub_job,
                orchestration_cls=stub_orchestration,
                dag=None,
                schedule=None,
                dependencies=None,
                orchestration=stub_orchestration,
            )


# ---------------------------------------------------------------------------
# 2.8  AbstractPipeline.pipeline_id construction
# ---------------------------------------------------------------------------

class TestAbstractPipelinePipelineId:
    def test_pipeline_id_from_filename(self, stub_job, stub_orchestration, stub_schedule):
        with stub_orchestration:
            pipeline = StubPipeline(
                job=stub_job,
                orchestration_cls=stub_orchestration,
                dag=None,
                schedule=stub_schedule,
                orchestration=stub_orchestration,
            )
            # filename is "test_pipeline.py" → "test_pipeline" + "_daily"
            # sanitize_id preserves hyphens, so schedule name "daily" is joined with "_"
            # but the actual code uses sanitize_id which keeps the name as-is
            assert "test_pipeline" in pipeline.pipeline_id
            assert "daily" in pipeline.pipeline_id

    def test_pipeline_id_without_schedule_name(self, stub_job, stub_orchestration):
        from ai.starlake.orchestration.starlake_schedules import StarlakeDomain, StarlakeTable
        schedule_no_name = StarlakeSchedule(
            name=None,
            cron="0 0 * * *",
            domains=[
                StarlakeDomain(
                    name="d",
                    final_name="d",
                    tables=[StarlakeTable(name="t")],
                )
            ],
        )
        with stub_orchestration:
            pipeline = StubPipeline(
                job=stub_job,
                orchestration_cls=stub_orchestration,
                dag=None,
                schedule=schedule_no_name,
                orchestration=stub_orchestration,
            )
            assert pipeline.pipeline_id == "test_pipeline"


# ---------------------------------------------------------------------------
# 2.9  AbstractPipeline @final task creation methods
# ---------------------------------------------------------------------------

class TestAbstractPipelineMethods:
    """Verify key pipeline methods exist and run() is abstract."""

    @pytest.mark.parametrize(
        "method_name",
        [
            "start_task",
            "end_task",
            "sl_load",
            "sl_transform",
            "sl_import",
            "sl_pre_load",
        ],
    )
    def test_method_exists_on_pipeline(self, method_name):
        assert hasattr(AbstractPipeline, method_name)
        assert callable(getattr(AbstractPipeline, method_name))

    def test_run_is_abstract(self):
        """run() is the only abstract method on AbstractPipeline."""
        import inspect
        assert inspect.isabstract(AbstractPipeline)
        method = getattr(AbstractPipeline, "run", None)
        assert method is not None
        assert getattr(method, "__isabstractmethod__", False)


# ---------------------------------------------------------------------------
# 2.10  AbstractEvent contract
# ---------------------------------------------------------------------------

class TestAbstractEventContract:
    def test_to_event_is_abstract(self):
        """to_event() is declared as @abstractmethod on AbstractEvent."""
        method = getattr(AbstractEvent, "to_event", None)
        assert method is not None
        assert getattr(method, "__isabstractmethod__", False)


# ---------------------------------------------------------------------------
# 2.11  TaskGroupContext enter/exit
# ---------------------------------------------------------------------------

class TestTaskGroupContextEnterExit:
    def test_current_context_none_initially(self):
        assert TaskGroupContext.current_context() is None

    def test_enter_sets_current_context(self, stub_orchestration):
        ctx = TaskGroupContext("test_group", orchestration_cls=stub_orchestration)
        with ctx:
            assert TaskGroupContext.current_context() is ctx
        assert TaskGroupContext.current_context() is None


# ---------------------------------------------------------------------------
# 2.12  TaskGroupContext nesting
# ---------------------------------------------------------------------------

class TestTaskGroupContextNesting:
    def test_nested_contexts(self, stub_orchestration):
        outer = TaskGroupContext("outer", orchestration_cls=stub_orchestration)
        with outer:
            assert outer.level == 1
            inner = TaskGroupContext("inner", orchestration_cls=stub_orchestration)
            with inner:
                assert inner.parent is outer
                assert inner.level == 2
                assert TaskGroupContext.current_context() is inner
            assert TaskGroupContext.current_context() is outer

    def test_parent_points_to_outer(self, stub_orchestration):
        outer = TaskGroupContext("outer", orchestration_cls=stub_orchestration)
        with outer:
            inner = TaskGroupContext("inner", orchestration_cls=stub_orchestration)
            with inner:
                assert inner.parent is outer


# ---------------------------------------------------------------------------
# 2.13  AbstractTask auto-registers with current TaskGroupContext
# ---------------------------------------------------------------------------

class TestAbstractTaskAutoRegisters:
    def test_task_registers_with_context(self, stub_orchestration):
        ctx = TaskGroupContext("group", orchestration_cls=stub_orchestration)
        with ctx:
            task = AbstractTask(task_id="my_task", task={"dummy": True})
            assert task in ctx.dependencies
            assert ctx.get_dependency("my_task") is task

    def test_task_without_context_raises(self):
        with pytest.raises(ValueError, match="No task group context found"):
            AbstractTask(task_id="orphan_task", task=None)
