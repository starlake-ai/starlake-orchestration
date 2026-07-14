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

from tests.airflow.airflow_test_mixin import AirflowTestMixin
from tests.shared.base_test_sl_load import BaseTestSlLoad


class TestAirflowSlLoad(AirflowTestMixin, BaseTestSlLoad):
    """Airflow 2 concrete implementation of sl_load() shared tests."""
