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

"""Version-agnostic Dataset/Asset aliases for the Airflow test modules.

Airflow 2 names (Dataset/DatasetAll/DatasetAny, timetable.dataset_condition)
map to Airflow 3 assets (Asset/AssetAll/AssetAny, timetable.asset_condition).
Extracted from test_airflow_runtime.py so every Airflow test module shares
one alias block.
"""

from __future__ import annotations

try:
    import airflow

    AIRFLOW_AVAILABLE = True
    AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
    SUPPORTS_ASSETS = AIRFLOW_VERSION >= (3, 0)
    if SUPPORTS_ASSETS:
        from airflow.sdk import Asset as Dataset
        from airflow.sdk import AssetAll as DatasetAll
        from airflow.sdk import AssetAny as DatasetAny
    else:
        from airflow.datasets import Dataset, DatasetAll, DatasetAny
except ImportError:  # pragma: no cover — collection guard only
    AIRFLOW_AVAILABLE = False
    AIRFLOW_VERSION = (0, 0)
    SUPPORTS_ASSETS = False
    Dataset = DatasetAll = DatasetAny = None

# Name of the condition attribute on dataset/asset-triggered timetables.
CONDITION_ATTR = "asset_condition" if SUPPORTS_ASSETS else "dataset_condition"
