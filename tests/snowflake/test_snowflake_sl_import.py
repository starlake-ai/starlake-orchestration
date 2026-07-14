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
from tests.shared.base_test_sl_import import BaseTestSlImport


class TestSnowflakeSlImport(SnowflakeTestMixin, BaseTestSlImport):
    """Snowflake sl_import() is NOT implemented for the SQL executor.

    ``sl_import()`` builds arguments without ``sink`` kwarg, causing
    ``sl_job()`` to raise ``ValueError("sink is required")``.
    Snowflake uses Streams for data ingestion, not stage/import commands.
    """

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_import not implemented for Snowflake SQL executor",
    )
    def test_import_single_domain(self):
        super().test_import_single_domain()

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_import not implemented for Snowflake SQL executor",
    )
    def test_import_with_tables_filter(self):
        super().test_import_with_tables_filter()

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_import not implemented for Snowflake SQL executor",
    )
    def test_import_task_id_format(self):
        super().test_import_task_id_format()
