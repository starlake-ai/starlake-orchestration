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
"""``cloud_run_async_poke_interval`` on BOTH asynchronous completion paths.

The option is documented once, without distinguishing them, and an
asynchronous cloud_run job is watched by one of two sensors depending on
``use_gcloud``. Only the gcloud one received the option: on the operator path
the sensor fell back to ``BaseSensorOperator``'s own 60 s default, so a
deployment setting the option watched its jobs at a cadence it never chose.
"""

from __future__ import annotations

import pytest

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME, AIRFLOW_AVAILABLE

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Requires Apache Airflow",
)

try:
    import airflow.providers.google.cloud.operators.cloud_run  # noqa: F401
    import google.cloud.run_v2  # noqa: F401
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False

google_only = pytest.mark.skipif(
    not GOOGLE_AVAILABLE,
    reason="Requires apache-airflow-providers-google",
)

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
    "pre_load_strategy": "imported",
}

FRAMEWORK_DEFAULT_POKE_INTERVAL = 30.0   # starlake's own default for the option
AIRFLOW_DEFAULT_POKE_INTERVAL = 60       # BaseSensorOperator's, the value to never inherit


def _dag():
    from datetime import datetime

    from airflow import DAG
    return DAG(dag_id="test_cloud_run_poke_interval", start_date=datetime(2024, 1, 1), schedule=None)


def _make_job(extra_options):
    from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
    return StarlakeAirflowCloudRunJob(
        filename="test_airflow_cloud_run_poke_interval.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options=dict(CLOUD_RUN_OPTIONS, cloud_run_async="true", **extra_options),
    )


def _completion_sensor(use_gcloud, poke_interval=None):
    options = {"use_gcloud": use_gcloud}
    if poke_interval is not None:
        options["cloud_run_async_poke_interval"] = poke_interval
    job = _make_job(options)
    with _dag() as dag:
        job.sl_load(task_id="load_customers", domain="starbake", table="customers")
    return dag.get_task("load_customers_wait.load_customers_check_completion")


# ---------------------------------------------------------------------------
# Provider-free — the option reaches both construction sites
# ---------------------------------------------------------------------------

def test_both_completion_sensors_are_given_the_option():
    """Neither site may rely on a default: one of them is Airflow's, not ours.

    Read from disk, not imported: CI installs no google provider, so this is
    the only layer of this file that runs there.
    """
    import os

    import ai.starlake.airflow as pkg
    path = os.path.join(os.path.dirname(pkg.__file__), "gcp", "starlake_airflow_cloud_run_job.py")
    with open(path) as f:
        source = f.read()
    assert source.count("poke_interval=self.cloud_run_async_poke_interval") == 2


# ---------------------------------------------------------------------------
# Provider-guarded — the constructed sensors carry it
# ---------------------------------------------------------------------------

@google_only
@pytest.mark.parametrize("use_gcloud", ["true", "false"])
def test_configured_interval_reaches_the_sensor(use_gcloud):
    sensor = _completion_sensor(use_gcloud, poke_interval="15")
    assert sensor.poke_interval == 15


@google_only
@pytest.mark.parametrize("use_gcloud", ["true", "false"])
def test_unset_option_falls_back_to_the_framework_default(use_gcloud):
    """30 s — starlake's documented default, never Airflow's 60 s."""
    sensor = _completion_sensor(use_gcloud)
    assert sensor.poke_interval == FRAMEWORK_DEFAULT_POKE_INTERVAL
    assert sensor.poke_interval != AIRFLOW_DEFAULT_POKE_INTERVAL


@google_only
def test_both_paths_agree_on_the_same_option():
    """The two sensors watch the same kind of job: same cadence, same knob."""
    gcloud = _completion_sensor("true", poke_interval="45")
    operator = _completion_sensor("false", poke_interval="45")
    assert gcloud.poke_interval == operator.poke_interval == 45
