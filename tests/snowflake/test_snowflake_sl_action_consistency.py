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
from tests.shared.base_test_sl_action_consistency import BaseTestSlActionConsistency


class TestSnowflakeSlActionConsistency(SnowflakeTestMixin, BaseTestSlActionConsistency):
    """Snowflake canonical sl_* contract.

    Runtime data equivalence (AC #3) deliberately EXCLUDES Snowflake:
    the SQL executor runs inside Snowflake and the test suite mocks the
    SDK — there is no local DuckDB state to compare. Snowflake
    participates through this structural contract and the NFR11
    error-message tests, which both PASS (core validation raises before
    sl_job is reached).
    """

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_import not implemented for Snowflake SQL executor",
    )
    def test_canonical_import_contract(self):
        super().test_canonical_import_contract()

    @pytest.mark.xfail(
        raises=(ValueError, NotImplementedError),
        strict=True,
        reason="sl_pre_load/sl_import not implemented for Snowflake SQL executor",
    )
    def test_canonical_pre_load_chain(self):
        super().test_canonical_pre_load_chain()
