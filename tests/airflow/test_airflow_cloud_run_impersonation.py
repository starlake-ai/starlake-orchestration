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

"""Issue #104 — cloud_run impersonation has TWO consumers with TWO formats.

``StarlakeAirflowCloudRunJob`` derives two values from
``cloud_run_service_account``:

- ``self.impersonate_service_account`` — the gcloud CLI fragment
  ``--impersonate-service-account <email>`` (or ``""``), interpolated into
  every gcloud/bash command string;
- the bare email (``self.cloud_run_service_account or None``), passed as the
  Google provider's ``impersonation_chain`` parameter on every python-operator
  site.

Before the fix, the CLI fragment was passed to BOTH consumers: any non-empty
service account reached the Google auth layer as the literal string
``--impersonate-service-account sa@...``, which it cannot use. Only
``use_gcloud=false`` deployments with a service account were affected.

Two layers, mirroring the 6.3/6.4/#99 suites:

- provider-free source-scan pin (runs in CI, which installs NO google
  provider): the fragment attribute no longer feeds any
  ``impersonation_chain=`` site and the bare-email form is present at the
  expected multiplicity;
- provider-guarded content tests (skipped without
  ``apache-airflow-providers-google``): constructed operators/sensors carry
  the bare email (or ``None`` without a service account), and every gcloud
  command string keeps the ``--impersonate-service-account`` flag.
"""

from __future__ import annotations

import os

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

SA = "sa@test-project.iam.gserviceaccount.com"
FLAG = f"--impersonate-service-account {SA}"

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
    "pre_load_strategy": "imported",
}

# the four core-injected sensor kwargs (6.5 preload-waiting construction)
SENSOR_OPTIONS = {
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "30",
    "pre_load_timeout": "120",
    "pre_load_sensor_soft_fail": "true",
}


def _dag():
    from airflow import DAG
    from datetime import datetime
    return DAG(dag_id="test_cloud_run_impersonation", start_date=datetime(2024, 1, 1), schedule=None)


def _make_job(extra_options=None, service_account=SA):
    from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
    options = dict(CLOUD_RUN_OPTIONS)
    if service_account is not None:
        options["cloud_run_service_account"] = service_account
    options.update(extra_options or {})
    return StarlakeAirflowCloudRunJob(
        filename="test_airflow_cloud_run_impersonation.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options=options,
    )


# ---------------------------------------------------------------------------
# 1. Provider-free — source-scan pin (runs where no google provider installs)
# ---------------------------------------------------------------------------

class TestImpersonationSourcePin:
    """CI installs no google provider, so the guarded content tests below
    never run there — this source scan is the CI-runnable guard. It proves
    text, not runtime behaviour (that is the guarded content tests' job); it
    keeps the CLI fragment from creeping back onto an ``impersonation_chain=``
    site (issue #104) and keeps the fragment construction the gcloud commands
    interpolate from being wrongly removed."""

    def _source(self):
        import ai.starlake.airflow as pkg
        path = os.path.join(os.path.dirname(pkg.__file__), "gcp", "starlake_airflow_cloud_run_job.py")
        with open(path) as f:
            return f.read()

    def test_fragment_feeds_no_impersonation_chain_site(self):
        assert "impersonation_chain=self.impersonate_service_account" not in self._source()

    def test_bare_email_sites_present_at_expected_multiplicity(self):
        # sync operator, async operator + completion sensor, and the 6.5
        # preload-waiting `common` dict (which feeds both the deferrable
        # operator and the sensor-flavor closure)
        source = self._source()
        assert source.count("impersonation_chain=self.cloud_run_service_account or None") == 4

    def test_gcloud_fragment_construction_still_present(self):
        assert 'f"--impersonate-service-account {self.cloud_run_service_account}"' in self._source()


# ---------------------------------------------------------------------------
# 2. Provider-guarded — python operators receive the bare email (AC1/AC2)
# ---------------------------------------------------------------------------

@google_only
class TestImpersonationChainBareEmail:

    def test_ctor_keeps_two_derived_values(self):
        job = _make_job()
        assert job.cloud_run_service_account == SA
        assert job.impersonate_service_account == FLAG

    def test_sync_api_operator(self):
        job = _make_job({"use_gcloud": "false", "cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert task.impersonation_chain == SA

    def test_async_api_operator_and_completion_sensor(self):
        job = _make_job({"use_gcloud": "false", "cloud_run_async": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert dag.get_task("load_customers_wait.load_customers").impersonation_chain == SA
        assert dag.get_task("load_customers_wait.load_customers_check_completion").impersonation_chain == SA

    def test_preload_waiting_deferrable_operator(self):
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import CloudRunJobOperator
        job = _make_job(dict(SENSOR_OPTIONS, use_gcloud="false"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, CloudRunJobOperator)
        assert task.deferrable is True
        assert task.impersonation_chain == SA

    def test_preload_waiting_sensor_closure(self, monkeypatch):
        """The sensor flavor builds an ad-hoc per-poke operator from the
        `common` dict captured in the `submit_and_wait` closure — the bare
        email must reach it too (same edit site as the deferrable flavor)."""
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        job = _make_job(dict(SENSOR_OPTIONS, use_gcloud="false", pre_load_deferrable="false"))
        with _dag():
            sensor = job.sl_pre_load(domain="starbake", tables={"customers"})
        seen = {}

        def capture(self, context):
            seen["chain"] = self.impersonation_chain
        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", capture)
        sensor._submit_and_wait({}, {"container_overrides": [{"args": []}]})
        assert seen["chain"] == SA

    def test_no_service_account_is_none_not_empty_string(self):
        """AC2 — ``None`` is the provider's documented "no impersonation"
        value; the pin is ``is None`` so the contract stays explicit."""
        job = _make_job({"use_gcloud": "false", "cloud_run_async": "false"}, service_account=None)
        assert job.cloud_run_service_account == ""
        assert job.impersonate_service_account == ""
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert task.impersonation_chain is None

    def test_no_service_account_async_and_deferrable_are_none(self):
        job = _make_job({"use_gcloud": "false", "cloud_run_async": "true"}, service_account=None)
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert dag.get_task("load_customers_wait.load_customers").impersonation_chain is None
        assert dag.get_task("load_customers_wait.load_customers_check_completion").impersonation_chain is None
        job = _make_job(dict(SENSOR_OPTIONS, use_gcloud="false"), service_account=None)
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task.impersonation_chain is None


# ---------------------------------------------------------------------------
# 3. Provider-guarded — gcloud command strings keep the CLI fragment (AC3)
# ---------------------------------------------------------------------------

@google_only
class TestGcloudFragmentUnchanged:

    def test_sync_command_keeps_flag(self):
        job = _make_job({"use_gcloud": "true", "cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert FLAG in task.bash_command

    def test_async_submission_status_and_sensor_keep_flag(self):
        job = _make_job({"use_gcloud": "true", "cloud_run_async": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert FLAG in dag.get_task("load_customers_wait.load_customers").bash_command
        assert FLAG in dag.get_task("load_customers_wait.load_customers_check_completion").bash_command
        assert FLAG in dag.get_task("load_customers_wait.load_customers_get_completion_status").bash_command

    def test_async_retry_on_failure_sensor_keeps_flag(self):
        job = _make_job({"use_gcloud": "true", "cloud_run_async": "true", "retry_on_failure": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert FLAG in dag.get_task("load_customers_wait.load_customers_check_completion").bash_command

    def test_preload_waiting_gcloud_sensor_keeps_flag(self):
        job = _make_job(dict(SENSOR_OPTIONS, use_gcloud="true"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert FLAG in task.bash_command

    def test_no_service_account_no_flag_anywhere(self):
        job = _make_job({"use_gcloud": "true", "cloud_run_async": "false"}, service_account=None)
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert "--impersonate-service-account" not in task.bash_command
        job = _make_job({"use_gcloud": "true", "cloud_run_async": "true"}, service_account=None)
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        for task_id in (
            "load_customers_wait.load_customers",
            "load_customers_wait.load_customers_check_completion",
            "load_customers_wait.load_customers_get_completion_status",
        ):
            assert "--impersonate-service-account" not in dag.get_task(task_id).bash_command, task_id
        job = _make_job(dict(SENSOR_OPTIONS, use_gcloud="true"), service_account=None)
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "--impersonate-service-account" not in task.bash_command
