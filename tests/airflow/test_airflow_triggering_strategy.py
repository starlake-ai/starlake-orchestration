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

"""Airflow triggering-strategy validation (dual-version: Airflow 2 and 3).

Structure tests (condition class family + member URIs) run identically on
both majors via the Dataset/Asset alias block; the ``evaluate()``
truth-table tests are Airflow-2-only (Airflow 3 removed ``evaluate()``
from the SDK asset classes — evaluation moved server-side).
"""

from __future__ import annotations

import pytest

from tests.airflow.airflow_test_mixin import AirflowTestMixin
from tests.airflow.dataset_compat import (
    AIRFLOW_AVAILABLE,
    CONDITION_ATTR,
    SUPPORTS_ASSETS,
    SUPPORTS_CONDITION_INTROSPECTION,
    Dataset,
    DatasetAll,
    DatasetAny,
)
from tests.shared.base_test_triggering_strategy import BaseTestTriggeringStrategy
from tests.shared.triggering_scenarios import (
    EXPECTED_URIS,
    NO_UPSTREAM,
    SINGLE_UPSTREAM,
    TWO_UPSTREAMS,
    make_dependencies,
)

# This module *introspects* the native condition object off the timetable
# (``.dataset_condition``/``.asset_condition``). The DatasetAny/DatasetAll
# operators exist from 2.9, but the timetable only exposes the condition from
# Airflow 2.10 on. Below 2.10 the ANY/ALL build/fallback behaviour is covered
# by the runtime/options tests instead (issue #125).
pytestmark = pytest.mark.skipif(
    not SUPPORTS_CONDITION_INTROSPECTION,
    reason="timetable condition introspection (dataset_condition) needs Airflow >= 2.10",
)


class TestAirflowTriggeringStrategy(AirflowTestMixin, BaseTestTriggeringStrategy):
    """Airflow concrete implementation of the shared triggering tests."""


def _make_pipeline(strategy, upstreams, filename):
    """Build an Airflow pipeline through the real orchestration path.

    Deliberately does NOT reuse ``AirflowTestMixin.create_orchestration``:
    the structure/semantics classes below are plain classes (no
    mixin/base inheritance), and per-test ``filename``s give each DAG a
    distinct ``pipeline_id`` (better failure messages).
    """
    from ai.starlake.airflow import AirflowOrchestration
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME

    job = StarlakeAirflowBashJob(
        filename=filename,
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options={"dataset_triggering_strategy": strategy},
    )
    orch = AirflowOrchestration(job=job)
    pipeline = orch.sl_create_pipeline(
        dependencies=make_dependencies(upstreams)
    )
    with pipeline:
        pass
    return pipeline


def _condition(pipeline):
    timetable = pipeline.dag.timetable
    assert hasattr(timetable, CONDITION_ATTR), (
        f"Timetable {type(timetable).__name__} has no {CONDITION_ATTR}"
    )
    return getattr(timetable, CONDITION_ATTR)


class TestAirflowTriggeringStructure:
    """Condition structure — runs identically on Airflow 2 and 3."""

    def test_any_two_upstreams_condition_is_or(self):
        pipeline = _make_pipeline("any", TWO_UPSTREAMS, "test_any_or.py")
        condition = _condition(pipeline)
        assert isinstance(condition, DatasetAny), (
            f"Expected {DatasetAny.__name__}, got {type(condition).__name__}"
        )
        assert {d.uri for d in condition.objects} == set(
            EXPECTED_URIS[TWO_UPSTREAMS]
        )

    def test_all_two_upstreams_condition_is_and(self):
        pipeline = _make_pipeline("all", TWO_UPSTREAMS, "test_all_and.py")
        condition = _condition(pipeline)
        assert isinstance(condition, DatasetAll), (
            f"Expected {DatasetAll.__name__}, got {type(condition).__name__}"
        )
        assert {d.uri for d in condition.objects} == set(
            EXPECTED_URIS[TWO_UPSTREAMS]
        )

    @pytest.mark.parametrize("strategy", ["any", "all"])
    def test_single_upstream_condition_is_bare_event(self, strategy):
        """reduce() over one event returns the event itself — for BOTH
        strategies the condition is a plain Dataset/Asset."""
        pipeline = _make_pipeline(
            strategy, SINGLE_UPSTREAM, f"test_single_{strategy}.py"
        )
        condition = _condition(pipeline)
        assert isinstance(condition, Dataset), (
            f"Expected bare {Dataset.__name__} for single upstream, "
            f"got {type(condition).__name__}"
        )
        assert condition.uri == "starbake_orders"

    @pytest.mark.parametrize("strategy", ["any", "all"])
    def test_no_upstream_dag_is_not_dataset_triggered(self, strategy):
        pipeline = _make_pipeline(
            strategy, NO_UPSTREAM, f"test_none_{strategy}.py"
        )
        assert pipeline.events == []
        timetable_cls = type(pipeline.dag.timetable).__name__
        assert "Dataset" not in timetable_cls and "Asset" not in timetable_cls, (
            f"DAG without upstreams must not be dataset-triggered, "
            f"got {timetable_cls}"
        )

    def test_mixed_strategies_independent_pipelines(self):
        """Two pipelines with different strategies diverge structurally."""
        p_any = _make_pipeline("any", TWO_UPSTREAMS, "test_mixed_any.py")
        p_all = _make_pipeline("all", TWO_UPSTREAMS, "test_mixed_all.py")
        assert isinstance(_condition(p_any), DatasetAny)
        assert isinstance(_condition(p_all), DatasetAll)


@pytest.mark.skipif(
    SUPPORTS_ASSETS,
    reason=(
        "Airflow 3 removed BaseAsset.evaluate() from the SDK — evaluation "
        "is server-side (AssetEvaluator + session). Semantics on 3.x are "
        "covered by the structural tests above (same class family, same "
        "member URIs, evaluated by the identical serialized agg_func)."
    ),
)
class TestAirflowTriggeringSemantics:
    """Truth-table on the native condition objects (Airflow 2 only)."""

    def test_any_triggers_on_first_available(self):
        pipeline = _make_pipeline("any", TWO_UPSTREAMS, "test_sem_any.py")
        condition = _condition(pipeline)
        first_uri = next(iter(EXPECTED_URIS[TWO_UPSTREAMS]))
        assert condition.evaluate({first_uri: True}) is True
        assert condition.evaluate({}) is False

    def test_all_waits_for_all(self):
        pipeline = _make_pipeline("all", TWO_UPSTREAMS, "test_sem_all.py")
        condition = _condition(pipeline)
        uris = set(EXPECTED_URIS[TWO_UPSTREAMS])
        first_uri = next(iter(uris))
        assert condition.evaluate({first_uri: True}) is False
        assert condition.evaluate({u: True for u in uris}) is True

    @pytest.mark.parametrize("strategy", ["any", "all"])
    def test_single_upstream_triggers_on_its_event(self, strategy):
        pipeline = _make_pipeline(
            strategy, SINGLE_UPSTREAM, f"test_sem_single_{strategy}.py"
        )
        condition = _condition(pipeline)
        assert condition.evaluate({"starbake_orders": True}) is True
        assert condition.evaluate({}) is False
