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

"""Issue #125 — Airflow < 2.10 dataset outlet ``extra`` propagation.

Before Airflow 2.10 there is no ``context["outlet_events"]`` accessor to carry
Starlake's runtime metadata onto an emitted ``DatasetEvent``. The producer-side
compat fix has two halves:

* ``StarlakeDatasetMixin.pre_execute`` pushes the rendered ``extra`` onto each
  outlet ``Dataset`` itself when ``supports_inlet_events()`` is ``False``.
* ``compat.install_dataset_extra_forwarding`` wraps
  ``DatasetManager.register_dataset_change`` so the default emission path
  forwards that outlet's own ``extra`` when the caller passes none.

The dev/CI environment runs Airflow >= 2.10, so the pre-2.10 path is *simulated*
by forcing ``supports_inlet_events()`` to ``False``; the branch logic itself is
version-independent Python.
"""

from __future__ import annotations

import datetime
import types

import pytest

from ai.starlake.airflow import compat
from ai.starlake.airflow.compat import Dataset, install_dataset_extra_forwarding
from ai.starlake.common import StarlakeParameters
import ai.starlake.airflow.starlake_airflow_job as jobmod
from ai.starlake.airflow.starlake_airflow_job import StarlakeEmptyOperator
from ai.starlake.dataset import StarlakeDataset

URI = StarlakeParameters.URI_PARAMETER.value
SINK = StarlakeParameters.SINK_PARAMETER.value
SCHEDULED_DATE = StarlakeParameters.SCHEDULED_DATE_PARAMETER.value


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def stub_super_pre_execute(monkeypatch):
    """Stub ``BaseOperator.pre_execute`` — the lineage-decorated Airflow tail.

    Isolates the mixin's own branch logic (the only thing issue #125 changed)
    from Airflow's ``@prepare_lineage`` machinery, which would otherwise need a
    live DAG run, XCom and a DB session.
    """
    from airflow.models.baseoperator import BaseOperator

    monkeypatch.setattr(BaseOperator, "pre_execute", lambda self, context: None)


def _build_operator(dataset):
    from airflow import DAG

    with DAG(dag_id="issue125", start_date=datetime.datetime(2024, 1, 1), schedule=None):
        return StarlakeEmptyOperator(task_id="t", dataset=dataset, source="src")


def _context():
    import pytz

    ti = types.SimpleNamespace(
        start_date=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC)
    )
    return {"ti": ti}


# ---------------------------------------------------------------------------
# Mixin — pre-2.10 fallback branch (Airflow 2.x concern; assets always carry
# the accessor so the fallback is meaningless on Airflow 3).
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    compat.supports_assets(),
    reason="pre-2.10 outlet-extra fallback is an Airflow 2.x concern",
)
class TestMixinPre210Fallback:

    def test_rendered_extra_pushed_onto_every_outlet(
        self, monkeypatch, stub_super_pre_execute
    ):
        monkeypatch.setattr(jobmod, "supports_inlet_events", lambda: False)
        op = _build_operator(StarlakeDataset(name="starbake.orders", cron="0 * * * *"))
        # render_template_fields replaces self.extra with a NEW dict at runtime,
        # breaking the by-reference link established in __init__; simulate that
        # so we prove pre_execute re-syncs the outlets to the rendered dict.
        op.extra = dict(op.extra)
        assert op.outlets and op.outlets[0].extra is not op.extra  # link broken

        op.pre_execute(_context())

        assert op.outlets
        for outlet in op.outlets:
            assert isinstance(outlet, Dataset)
            assert outlet.extra is op.extra                 # re-synced
            assert outlet.extra["ts"]                       # runtime ts attached
            assert outlet.extra[URI] == "starbake_orders"
            assert outlet.extra[SINK] == "starbake.orders"
            assert outlet.extra[SCHEDULED_DATE]             # scheduled-date carried

    def test_outlet_events_accessor_not_touched(
        self, monkeypatch, stub_super_pre_execute
    ):
        monkeypatch.setattr(jobmod, "supports_inlet_events", lambda: False)
        op = _build_operator("starbake.orders")  # plain string dataset, no cron

        class _Boom(dict):
            def __getitem__(self, key):  # pragma: no cover - must never run
                raise AssertionError(
                    "outlet_events accessor must not be used on the pre-2.10 path"
                )

        ctx = _context()
        ctx["outlet_events"] = _Boom()

        op.pre_execute(ctx)  # must not raise

        assert op.outlets[0].extra["ts"]
        assert op.outlets[0].extra["source"] == "src"


# ---------------------------------------------------------------------------
# Mixin — native 2.10+ path stays unchanged (regression guard).
# ---------------------------------------------------------------------------

class TestMixin210Accessor:

    def test_extra_set_via_outlet_events_accessor(
        self, monkeypatch, stub_super_pre_execute
    ):
        monkeypatch.setattr(jobmod, "supports_inlet_events", lambda: True)
        op = _build_operator(StarlakeDataset(name="starbake.orders", cron="0 * * * *"))
        op.extra = dict(op.extra)
        init_outlet = op.outlets[0]

        events = {}

        class _Accessor:
            def __getitem__(self, outlet):
                return events.setdefault(
                    id(outlet), types.SimpleNamespace(extra=None)
                )

        ctx = _context()
        ctx["outlet_events"] = _Accessor()

        op.pre_execute(ctx)

        assert events, "the 2.10+ path must go through the outlet_events accessor"
        for event in events.values():
            assert event.extra is op.extra
        # native path must NOT mutate the __init__-time outlet's own extra
        assert init_outlet.extra is not op.extra


# ---------------------------------------------------------------------------
# compat.install_dataset_extra_forwarding — register_dataset_change wrapper.
# DatasetManager only exists on Airflow 2.4-2.x (assets.manager on 3.x).
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not compat.supports_datasets(),
    reason="DatasetManager only exists on Airflow 2.4-2.x",
)
class TestRegisterDatasetChangeForwarding:

    @pytest.fixture
    def spy_manager(self, monkeypatch):
        """Force the pre-2.10 install path and swap in a recording stub as the
        ``register_dataset_change`` the wrapper will delegate to."""
        from airflow.datasets.manager import DatasetManager

        monkeypatch.setattr(compat, "supports_inlet_events", lambda: False)
        monkeypatch.setattr(compat, "supports_datasets", lambda: True)

        recorded = {"calls": 0}

        def spy(self, *, task_instance=None, dataset=None, extra=None, session=None, **kwargs):
            recorded["calls"] += 1
            recorded["extra"] = extra
            recorded["dataset"] = dataset
            return "event"

        monkeypatch.setattr(DatasetManager, "register_dataset_change", spy)
        return DatasetManager, recorded

    def test_forwards_outlet_extra_when_none_passed(self, spy_manager):
        DatasetManager, recorded = spy_manager
        assert install_dataset_extra_forwarding() is True

        mgr = object.__new__(DatasetManager)
        DatasetManager.register_dataset_change(
            mgr, task_instance=None, dataset=Dataset(uri="d", extra={URI: "d"}), session=None
        )

        assert recorded["extra"] == {URI: "d"}
        assert recorded["calls"] == 1

    def test_explicit_extra_is_preserved(self, spy_manager):
        DatasetManager, recorded = spy_manager
        install_dataset_extra_forwarding()

        mgr = object.__new__(DatasetManager)
        DatasetManager.register_dataset_change(
            mgr,
            task_instance=None,
            dataset=Dataset(uri="d", extra={URI: "d"}),
            extra={"a": 1},
            session=None,
        )

        assert recorded["extra"] == {"a": 1}

    def test_explicit_empty_extra_is_preserved(self, spy_manager):
        DatasetManager, recorded = spy_manager
        install_dataset_extra_forwarding()

        mgr = object.__new__(DatasetManager)
        # An explicit empty dict is a deliberate "no extra" — it must NOT be
        # overwritten by the dataset's own extra (the wrapper tests ``is None``).
        DatasetManager.register_dataset_change(
            mgr,
            task_instance=None,
            dataset=Dataset(uri="d", extra={URI: "d"}),
            extra={},
            session=None,
        )

        assert recorded["extra"] == {}

    def test_install_is_idempotent(self, spy_manager):
        DatasetManager, _ = spy_manager
        install_dataset_extra_forwarding()
        wrapped = DatasetManager.register_dataset_change
        assert install_dataset_extra_forwarding() is True
        assert DatasetManager.register_dataset_change is wrapped  # not re-wrapped

    def test_noop_when_inlet_events_supported(self, monkeypatch):
        monkeypatch.setattr(compat, "supports_inlet_events", lambda: True)
        assert install_dataset_extra_forwarding() is False
