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

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_sl_pre_load import BaseTestSlPreLoad


class TestSnowflakeSlPreLoad(SnowflakeTestMixin, BaseTestSlPreLoad):
    """Snowflake sl_pre_load() is NOT implemented for the SQL executor.

    ``sl_pre_load()`` builds arguments without ``sink`` kwarg, causing
    ``sl_job()`` to raise ``ValueError("sink is required")``.
    The NONE strategy returns None before reaching ``sl_job()`` and passes.
    """

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_pre_load not implemented for Snowflake SQL executor",
    )
    def test_pre_load_strategy_imported(self):
        super().test_pre_load_strategy_imported()

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_pre_load not implemented for Snowflake SQL executor",
    )
    def test_pre_load_strategy_pending(self):
        super().test_pre_load_strategy_pending()

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_pre_load not implemented for Snowflake SQL executor",
    )
    def test_pre_load_strategy_ack(self):
        super().test_pre_load_strategy_ack()

    # test_pre_load_strategy_none passes — NONE returns None before sl_job()
