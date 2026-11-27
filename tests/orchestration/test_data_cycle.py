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

"""``data_cycle`` option normalization (issue #135).

The constructor used to assign the raw option value straight to the private
field, bypassing the ``data_cycle`` setter — with the ``"none"`` default the
literal string survived, and ``check_datasets`` (``if self.data_cycle:`` —
``"none"`` is truthy) crashed the start task of every dataset-triggered DAG
with ``ValueError: Invalid cron expression: none``. The constructor now routes
through the setter, and the getter enforces the same ``data_cycle_enabled``
gate as the setter.
"""

from __future__ import annotations

import pytest

from tests.orchestration.conftest import StubJob, _STUB_MODULE_NAME


def _job(options):
    return StubJob(filename="test.py", module_name=_STUB_MODULE_NAME, options=options)


class TestDataCycleNormalization:
    def test_default_is_none_not_the_string(self):
        """The regression: with no data_cycle option at all, the property must
        be None — not the literal "none" default that get_cron_frequency
        rejects at the first dataset-triggered run."""
        job = _job({})
        assert job.data_cycle is None

    def test_disabled_ignores_any_value(self):
        job = _job({"data_cycle": "daily"})   # data_cycle_enabled defaults to false
        assert job.data_cycle is None

    def test_enabled_none_string_is_none(self):
        job = _job({"data_cycle_enabled": "true", "data_cycle": "none"})
        assert job.data_cycle is None

    @pytest.mark.parametrize(
        ("preset", "cron"),
        [
            ("hourly", "0 * * * *"),
            ("daily", "0 0 * * *"),
            ("weekly", "0 0 * * 0"),
            ("monthly", "0 0 1 * *"),
            ("yearly", "0 0 1 1 *"),
        ],
    )
    def test_enabled_presets_normalize_to_cron(self, preset, cron):
        job = _job({"data_cycle_enabled": "true", "data_cycle": preset})
        assert job.data_cycle == cron

    def test_enabled_raw_cron_passes_through(self):
        job = _job({"data_cycle_enabled": "true", "data_cycle": "0 6 * * *"})
        assert job.data_cycle == "0 6 * * *"

    def test_enabled_invalid_value_fails_loudly_at_parse(self):
        """An invalid data_cycle must raise at DAG-parse time (job build), not
        crash the start task at the first triggered run."""
        with pytest.raises(ValueError, match="Invalid data cycle"):
            _job({"data_cycle_enabled": "true", "data_cycle": "not-a-cron"})


class TestDataCycleDeclared:
    """Issue #167 — only a cycle declared by the data_cycle option states the
    period of the data; the default one, assigned from the pipeline's computed
    cron, carries the hour of one dependency's cron."""

    def test_declared_by_the_option(self):
        assert _job({"data_cycle_enabled": "true", "data_cycle": "monthly"}).data_cycle_declared

    def test_not_declared_without_the_option(self):
        assert not _job({"data_cycle_enabled": "true"}).data_cycle_declared

    def test_not_declared_when_disabled(self):
        assert not _job({"data_cycle": "monthly"}).data_cycle_declared

    def test_the_default_cycle_of_the_pipeline_is_not_declared(self, stub_schedule):
        from tests.orchestration.conftest import StubOrchestration
        job = _job({"data_cycle_enabled": "true"})
        orchestration = StubOrchestration(job=job)
        with orchestration:
            with orchestration.sl_create_pipeline(schedule=stub_schedule):
                pass
        assert job.data_cycle == stub_schedule.cron
        assert not job.data_cycle_declared
        assert not job.data_cycle_mode("0 0 * * *")


class TestFiresInEveryPeriod:
    @pytest.mark.parametrize(
        ("cron", "data_cycle", "expected"),
        [
            ("0 6 1-3 * *", "0 0 1 * *", True),   # three attempts of one monthly delivery
            ("0 6 * * 1-5", "0 0 * * 0", True),   # five attempts of one weekly delivery
            ("0 6 * * 1-5", "0 0 * * *", False),  # weekends: five daily deliveries
            ("0 6 2 1 *", "0 0 1 * *", False),    # yearly
            ("0 6 29 * *", "0 0 1 * *", False),   # no 29 February in common years
            ("0 6 2 * *", "0 0 1 * *", True),     # once per period
            ("0 6 * * *", "0 0 * * *", True),
            ("0 6 1,2 1 *", "0 0 1 * *", False),  # twice in January, never after
            # a tick on the period's end closes the period that ends there: a
            # dependency firing only on the data-cycle ticks keeps its window
            ("0 0 1 * *", "0 0 1 * *", False),
            ("0 0 * * *", "0 0 * * *", False),
            ("0 6 1-3 * *", "0 6 1-3 * *", False),  # the data cycle itself
            ("0 0 1,15 * *", "0 0 1 * *", True),    # the 15th lies inside every period
            ("0 6 1-3 * *", "0 0 1 * * * 2030", False),  # no period in the reference span
            (None, "0 0 1 * *", False),
            ("0 6 * * *", None, False),
            ("not a cron", "0 0 1 * *", False),
        ],
    )
    def test_mode_table(self, cron, data_cycle, expected):
        from ai.starlake.common import fires_in_every_period
        assert fires_in_every_period(cron, data_cycle) is expected

    def test_never_reads_the_wall_clock(self, monkeypatch):
        import ai.starlake.common as common

        def wall_clock(*args, **kwargs):
            raise AssertionError("get_cron_frequency measures the gaps from now")

        monkeypatch.setattr(common, "get_cron_frequency", wall_clock)
        common.fires_in_every_period.cache_clear()
        assert common.fires_in_every_period("0 6 1-3 * *", "0 0 1 * *")
        assert not common.fires_in_every_period("0 6 2 1 *", "0 0 1 * *")


class TestDataCycleMode:
    def test_declared_cycle_and_a_dependency_firing_in_every_period(self):
        job = _job({"data_cycle_enabled": "true", "data_cycle": "monthly"})
        assert job.data_cycle_mode("0 6 1-3 * *")
        assert not job.data_cycle_mode("0 6 2 1 *")
        assert not job.data_cycle_mode(None)

    def test_without_data_cycle(self):
        assert not _job({"data_cycle": "monthly"}).data_cycle_mode("0 6 1-3 * *")
        assert not _job({}).data_cycle_mode("0 6 1-3 * *")


class TestDataCycleWindow:
    def test_from_the_start_of_the_period_that_holds_the_anchor(self):
        from datetime import datetime, timezone
        from ai.starlake.common import data_cycle_window
        anchor = datetime(2026, 12, 2, 6, tzinfo=timezone.utc)
        assert data_cycle_window("0 0 1 * *", anchor) == (datetime(2026, 12, 1, tzinfo=timezone.utc), anchor)

    def test_an_anchor_on_a_tick_closes_the_period_that_ends_there(self):
        from datetime import datetime, timezone
        from ai.starlake.common import data_cycle_window
        anchor = datetime(2026, 12, 1, tzinfo=timezone.utc)
        assert data_cycle_window("0 0 1 * *", anchor) == (datetime(2026, 11, 1, tzinfo=timezone.utc), anchor)
