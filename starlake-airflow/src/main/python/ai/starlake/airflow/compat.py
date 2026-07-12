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

"""Compatibility layer between Airflow 2.x and Airflow 3.x.

All import fallbacks and version checks required to support both major
versions live here, so the rest of the package imports version-agnostic
names from a single place.
"""

import airflow

from packaging.version import Version, parse

try:
    from airflow.sdk.bases.hook import BaseHook  # Airflow 3.x
except ImportError:
    from airflow.hooks.base import BaseHook  # Airflow 2.x

try:
    from airflow.sdk import Asset as Dataset  # Airflow 3.x
except ImportError:
    from airflow.datasets import Dataset  # Airflow 2.4+

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3.x
except ImportError:
    from airflow.models.baseoperator import BaseOperator  # Airflow 2.x

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue  # Airflow 3.x
except ImportError:
    from airflow.sensors.base import BaseSensorOperator, PokeReturnValue  # Airflow 2.x

try:
    from airflow.sdk import TaskGroup  # Airflow 3.x
except ImportError:
    from airflow.utils.task_group import TaskGroup  # Airflow 2.x

try:
    from airflow.sdk import get_current_context  # Airflow 3.x
except ImportError:
    from airflow.operators.python import get_current_context  # Airflow 2.x

try:
    from airflow.providers.standard.operators.empty import EmptyOperator  # Airflow 3.x
except ImportError:
    from airflow.operators.empty import EmptyOperator  # Airflow 2.x

try:
    from airflow.providers.standard.operators.bash import BashOperator  # Airflow 3.x
except ImportError:
    from airflow.operators.bash import BashOperator  # Airflow 2.x

try:
    from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator  # Airflow 3.x
except ImportError:
    from airflow.operators.python import PythonOperator, ShortCircuitOperator  # Airflow 2.x

try:
    from airflow.providers.standard.sensors.bash import BashSensor  # Airflow 3.x
except ImportError:
    from airflow.sensors.bash import BashSensor  # Airflow 2.x

try:
    from airflow.task.trigger_rule import TriggerRule  # Airflow 3.x
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # Airflow 2.x

__all__ = [
    "BaseHook",
    "Dataset",
    "BaseOperator",
    "BaseSensorOperator",
    "PokeReturnValue",
    "TaskGroup",
    "get_current_context",
    "EmptyOperator",
    "BashOperator",
    "PythonOperator",
    "ShortCircuitOperator",
    "BashSensor",
    "TriggerRule",
    "airflow_version",
    "supports_datasets",
    "supports_inlet_events",
    "supports_assets",
    "api_prefix",
]


def airflow_version() -> Version:
    """Return the running Airflow version."""
    return parse(airflow.__version__)


def supports_datasets() -> bool:
    """Datasets were introduced in Airflow 2.4 and replaced by assets in Airflow 3.0."""
    return parse("2.4.0") <= airflow_version() < parse("3.0.0")


def supports_inlet_events() -> bool:
    """Inlet events were introduced in Airflow 2.10."""
    return airflow_version() >= parse("2.10.0")


def supports_assets() -> bool:
    """Assets replace datasets starting with Airflow 3.0."""
    return airflow_version() >= parse("3.0.0")


def api_prefix() -> str:
    """REST API prefix: /api/v1 on Airflow 2.x, /api/v2 on Airflow 3.x."""
    return "/api/v2" if supports_assets() else "/api/v1"
