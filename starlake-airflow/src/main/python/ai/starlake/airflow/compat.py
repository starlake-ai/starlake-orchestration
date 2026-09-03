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

import functools

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

try:
    from airflow.sdk.exceptions import AirflowSkipException  # Airflow 3.x
except ImportError:
    from airflow.exceptions import AirflowSkipException  # Airflow 2.x

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
    "AirflowSkipException",
    "airflow_version",
    "supports_datasets",
    "supports_inlet_events",
    "supports_dataset_conditions",
    "supports_bash_retry_exit_code",
    "supports_assets",
    "api_prefix",
    "install_dataset_extra_forwarding",
    "ti_xcom_pull",
    "ti_xcom_push",
]


def airflow_version() -> Version:
    """Return the running Airflow version."""
    return parse(airflow.__version__)


def ti_xcom_pull(context, **kwargs):
    """Pull an XCom through the task instance carried by the context.

    Airflow 2 exposed ``xcom_pull(context, ...)`` on ``BaseOperator`` (and so
    on ``BaseSensorOperator``); the Airflow 3 Task SDK bases have neither
    ``xcom_pull`` nor ``xcom_push``, so ``self.xcom_pull(...)`` raises
    ``AttributeError`` at task runtime. The task instance itself carries the
    same call on both majors — ``ti.xcom_pull(task_ids=..., key=...)`` — and
    is what the Airflow 2 operator method delegated to.
    """
    return context["ti"].xcom_pull(**kwargs)


def ti_xcom_push(context, **kwargs):
    """Push an XCom through the task instance carried by the context.

    Same reason as :func:`ti_xcom_pull`: ``self.xcom_push`` exists on Airflow 2
    operators only. ``ti.xcom_push(key=..., value=...)`` is available on both.
    """
    context["ti"].xcom_push(**kwargs)


def supports_datasets() -> bool:
    """Datasets were introduced in Airflow 2.4 and replaced by assets in Airflow 3.0."""
    return parse("2.4.0") <= airflow_version() < parse("3.0.0")


def supports_inlet_events() -> bool:
    """Inlet events were introduced in Airflow 2.10."""
    return airflow_version() >= parse("2.10.0")


def supports_dataset_conditions() -> bool:
    """Conditional dataset scheduling — ``DatasetAny``/``DatasetAll`` via the
    ``|`` and ``&`` operators — was introduced in Airflow 2.9. Before 2.9 a DAG
    can only be scheduled on a *flat list* of datasets (native ALL semantics);
    ANY (OR) is not expressible."""
    return airflow_version() >= parse("2.9.0")


def supports_bash_retry_exit_code() -> bool:
    """``BashOperator``/``BashSensor`` gained the ``retry_exit_code`` parameter
    in Airflow 2.10."""
    return airflow_version() >= parse("2.10.0")


def supports_assets() -> bool:
    """Assets replace datasets starting with Airflow 3.0."""
    return airflow_version() >= parse("3.0.0")


def api_prefix() -> str:
    """REST API prefix: /api/v1 on Airflow 2.x, /api/v2 on Airflow 3.x."""
    return "/api/v2" if supports_assets() else "/api/v1"


def install_dataset_extra_forwarding() -> bool:
    """Airflow < 2.10 producer-side compat: forward an outlet ``Dataset``'s own
    ``extra`` onto the emitted ``DatasetEvent``.

    Before Airflow 2.10 there is no ``outlet_events`` runtime accessor, and the
    default task emission path calls ``DatasetManager.register_dataset_change``
    **without** an ``extra`` — so every task-emitted ``DatasetEvent.extra`` is
    ``{}``. ``StarlakeDatasetMixin.pre_execute`` stashes the rendered ``extra``
    on the outlet ``Dataset`` itself; this thin wrapper reads it back when the
    caller passes none, producing exactly one event carrying the runtime extra
    with no duplicate emission and no bypass of the internal-API path.

    Only installs on Airflow 2.4–2.9 (datasets present, inlet events absent);
    a no-op on 2.10+ (native accessor) and 3.x (assets). Idempotent — safe to
    call more than once. Returns ``True`` when the wrapper is (or already was)
    in place, ``False`` when this Airflow version needs no wrapper.

    Note: the wrapper patches ``DatasetManager.register_dataset_change``
    process-wide, so it applies to every outlet ``Dataset`` in the process, not
    only Starlake's. In practice it is a no-op unless a dataset carries its own
    ``extra`` (stock Airflow < 2.10 emits ``{}``): it forwards that declared
    ``extra`` onto the event, which is the behaviour datasets with an ``extra``
    intend. A deployment installing a custom ``[core] dataset_manager_class``
    that overrides ``register_dataset_change`` on a subclass is not wrapped.
    """
    if supports_inlet_events() or not supports_datasets():
        return False

    from airflow.datasets.manager import DatasetManager

    original = DatasetManager.register_dataset_change
    if getattr(original, "__starlake_extra_forwarding__", False):
        return True  # already installed

    @functools.wraps(original)
    def register_dataset_change(self, *args, dataset=None, extra=None, **kwargs):
        # Default emission path passes no extra pre-2.10; fall back to the
        # extra the mixin stashed on the outlet Dataset itself. Test ``is None``
        # (not falsiness) so an explicit empty ``extra={}`` from a caller is
        # honoured rather than overwritten by the dataset's own extra.
        if extra is None:
            extra = getattr(dataset, "extra", None)
        return original(self, *args, dataset=dataset, extra=extra, **kwargs)

    register_dataset_change.__starlake_extra_forwarding__ = True
    DatasetManager.register_dataset_change = register_dataset_change
    return True


# Install the producer-side wrapper eagerly so it is in place in the worker
# process before any task emits an outlet event. No-op on 2.10+/3.x.
install_dataset_extra_forwarding()
