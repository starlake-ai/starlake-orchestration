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
