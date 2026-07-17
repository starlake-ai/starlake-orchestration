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

"""Issue #99 (+ companion #101) — cloud engines must ship an UNQUOTED
``--scheduledDate`` to the container/driver CLI.

The LOAD/TRANSFORM ``--scheduledDate`` value used to be built single-quoted
(``'{{...}}'``), but none of the cloud engines' consumption paths has a shell
that consumes those quotes:

- Cloud Run gcloud embeds the value inside the double-quoted ``--args "..."``
  (bash keeps single quotes inside double quotes);
- Cloud Run API / Dataproc ``spark_job.args`` / Fargate ECS ``command`` hand
  the argument list to the container verbatim (exec form, no shell).

Literal quotes therefore reached the container CLI: ``LoadCmd`` strips them but
``TransformCmd`` does not, so a TRANSFORM run got a quoted scheduledDate in SQL
substitution/audit. The **bash job keeps its quotes** — a real shell runs its
command and consumes them.

Two layers, mirroring the 6.3/6.4 suites:

- provider-free source-scan pin (runs in CI, which installs NO google/amazon
  providers): the single quotes are gone from the three cloud-engine files and
  still present in the bash job;
- provider-guarded content tests (skipped without
  ``apache-airflow-providers-google`` / ``-amazon``): the actual arguments
  reaching gcloud ``--args``, the Cloud Run / Fargate ``overrides`` and the
  Dataproc ``spark_job.args`` carry no literal quote characters.
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

try:
    import airflow.providers.amazon.aws.operators.ecs  # noqa: F401
    AMAZON_AVAILABLE = True
except Exception:
    AMAZON_AVAILABLE = False

google_only = pytest.mark.skipif(
    not GOOGLE_AVAILABLE,
    reason="Requires apache-airflow-providers-google",
)
amazon_only = pytest.mark.skipif(
    not AMAZON_AVAILABLE,
    reason="Requires apache-airflow-providers-amazon",
)

# The template body keeps its own strftime('...') quotes; only the surrounding
# quotes of the value are removed. The value must start with the raw template.
TEMPLATE_HEAD = "{{sl_scheduled_date(params.cron"

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
    "pre_load_strategy": "imported",
}

DATAPROC_OPTIONS = {
    "spark_jar_list": "gs://test-bucket/starlake.jar",
    "spark_bucket": "test-bucket",
    "dataproc_project_id": "test-project",
    "dataproc_region": "europe-west1",
    "pre_load_strategy": "imported",
}

FARGATE_OPTIONS = {
    "aws_cluster_name": "test-cluster",
    "aws_task_definition_name": "test-task-def",
    "aws_task_definition_container_name": "test-container",
    "pre_load_strategy": "imported",
}


def _dag():
    from airflow import DAG
    from datetime import datetime
    return DAG(dag_id="test_cloud_run_scheduled_date", start_date=datetime(2024, 1, 1), schedule=None)


def _scheduled_date_value(args):
    """Return the value element that follows ``--scheduledDate`` in an argv list."""
    i = args.index("--scheduledDate")
    return args[i + 1]


def _assert_unquoted(value, expected_head=TEMPLATE_HEAD):
    assert not value.startswith("'"), f"leading literal quote: {value!r}"
    assert not value.endswith("'"), f"trailing literal quote: {value!r}"
    assert value.startswith(expected_head), value


# ---------------------------------------------------------------------------
# 1. Provider-free — source-scan pin (runs where no cloud provider installs)
# ---------------------------------------------------------------------------

class TestScheduledDateQuotingSourcePin:
    """CI installs no google/amazon provider, so the guarded content tests
    below never run there — this source scan is the CI-runnable guard. It
    proves text, not runtime behaviour (that is the guarded content tests'
    job); it keeps the single quotes from creeping back onto the cloud
    engines (issue #99 / #101) and keeps the bash job's quotes from being
    wrongly removed. The wrapping-quote checks are escaping-agnostic (``\\'``
    and ``'`` in the Python source compare equal) so a behaviour-preserving
    escape-style refactor does not flip the result, and a quote reintroduced
    without the backslash is still caught."""

    def _pkg_source(self, *relparts):
        import ai.starlake.airflow as pkg
        path = os.path.join(os.path.dirname(pkg.__file__), *relparts)
        with open(path) as f:
            # drop backslash-escapes before a quote so ``\'`` and ``'`` match
            return f.read().replace("\\'", "'")

    # wrapping-quote forms (normalized): the single-quoted Jinja default and
    # the single-quoted explicit override that used to wrap the value
    _WRAPPED_TEMPLATE = "'{{sl_scheduled_date"
    _WRAPPED_OVERRIDE = "f\"'{scheduled_date}'\""

    @pytest.mark.parametrize("relparts", [
        ("gcp", "starlake_airflow_cloud_run_job.py"),
        ("gcp", "starlake_airflow_dataproc_job.py"),
        ("aws", "starlake_airflow_fargate_job.py"),
    ])
    def test_cloud_engine_scheduled_date_is_unquoted(self, relparts):
        source = self._pkg_source(*relparts)
        # no wrapping quote around either the Jinja default or the override
        assert self._WRAPPED_TEMPLATE not in source, relparts
        assert self._WRAPPED_OVERRIDE not in source, relparts
        # the unquoted forms are present (the value starts right at the Python
        # string's opening double quote — nothing wraps it)
        assert '"{{sl_scheduled_date(params.cron' in source, relparts
        assert 'f"{scheduled_date}"' in source, relparts

    def test_bash_job_keeps_its_quotes(self):
        """The bash job runs its command through a real shell, which consumes
        the quotes — they are correct there and must NOT be removed."""
        source = self._pkg_source("bash", "starlake_airflow_bash_job.py")
        assert self._WRAPPED_TEMPLATE in source
        assert self._WRAPPED_OVERRIDE in source


# ---------------------------------------------------------------------------
# 2. Cloud Run — provider-guarded content tests
# ---------------------------------------------------------------------------

@google_only
class TestCloudRunScheduledDateUnquoted:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
        options = dict(CLOUD_RUN_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowCloudRunJob(
            filename="test_cloud_run_scheduled_date.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_gcloud_async_submission_command_unquoted(self):
        job = self._make_job()
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        cmd = dag.get_task("load_customers_wait.load_customers").bash_command
        assert "--scheduledDate {{sl_scheduled_date" in cmd
        assert "--scheduledDate '{{" not in cmd

    def test_gcloud_sync_raw_command_unquoted(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        cmd = task.bash_command
        assert "--scheduledDate {{sl_scheduled_date" in cmd
        assert "--scheduledDate '{{" not in cmd

    def test_gcloud_sync_wrapped_command_unquoted(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                do_xcom_push=True,
            )
        cmd = task.bash_command
        assert "--scheduledDate {{sl_scheduled_date" in cmd
        assert "--scheduledDate '{{" not in cmd

    def test_gcloud_transform_command_unquoted(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_transform(task_id="agg_orders", transform_name="starbake.agg_orders")
        cmd = task.bash_command
        assert "--scheduledDate {{sl_scheduled_date" in cmd
        assert "--scheduledDate '{{" not in cmd

    def test_gcloud_explicit_scheduled_date_unquoted(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                scheduled_date="2026-01-01T00:00:00+0000",
            )
        cmd = task.bash_command
        assert "--scheduledDate 2026-01-01T00:00:00+0000" in cmd
        assert "--scheduledDate '2026-01-01T00:00:00+0000'" not in cmd

    def test_api_path_args_unquoted(self):
        job = self._make_job({"use_gcloud": "false", "cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        args = task.overrides["container_overrides"][0]["args"]
        _assert_unquoted(_scheduled_date_value(args))

    def test_api_path_explicit_scheduled_date_unquoted(self):
        job = self._make_job({"use_gcloud": "false", "cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                scheduled_date="2026-01-01T00:00:00+0000",
            )
        args = task.overrides["container_overrides"][0]["args"]
        assert _scheduled_date_value(args) == "2026-01-01T00:00:00+0000"

    def test_api_path_async_args_unquoted(self):
        """The async API branch builds container_overrides["args"] inside a
        TaskGroup — same arguments list as the sync branch, exercised here so
        the matrix "sync or async" row is honestly covered."""
        job = self._make_job({"use_gcloud": "false", "cloud_run_async": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        op = dag.get_task("load_customers_wait.load_customers")
        args = op.overrides["container_overrides"][0]["args"]
        _assert_unquoted(_scheduled_date_value(args))

    def test_gcloud_preload_has_no_scheduled_date(self):
        """--scheduledDate is injected only for LOAD/TRANSFORM; a PRELOAD task
        must carry none (regression guard for the injection gate)."""
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "--scheduledDate" not in task.bash_command


# ---------------------------------------------------------------------------
# 3. Dataproc — provider-guarded content test
# ---------------------------------------------------------------------------

@google_only
class TestDataprocScheduledDateUnquoted:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.gcp.starlake_airflow_dataproc_job import StarlakeAirflowDataprocJob
        options = dict(DATAPROC_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowDataprocJob(
            filename="test_cloud_run_scheduled_date.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_spark_job_args_unquoted(self):
        job = self._make_job()
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        args = task.job["spark_job"]["args"]
        _assert_unquoted(_scheduled_date_value(args))

    def test_transform_spark_job_args_unquoted(self):
        job = self._make_job()
        with _dag():
            task = job.sl_transform(task_id="agg_orders", transform_name="starbake.agg_orders")
        args = task.job["spark_job"]["args"]
        _assert_unquoted(_scheduled_date_value(args))

    def test_explicit_scheduled_date_unquoted(self):
        job = self._make_job()
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                scheduled_date="2026-01-01T00:00:00+0000",
            )
        args = task.job["spark_job"]["args"]
        assert _scheduled_date_value(args) == "2026-01-01T00:00:00+0000"


# ---------------------------------------------------------------------------
# 4. Fargate — provider-guarded content test
# ---------------------------------------------------------------------------

@amazon_only
class TestFargateScheduledDateUnquoted:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.aws import StarlakeAirflowFargateJob
        options = dict(FARGATE_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowFargateJob(
            filename="test_cloud_run_scheduled_date.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_ecs_command_unquoted(self):
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        command = task.overrides["containerOverrides"][0]["command"]
        _assert_unquoted(_scheduled_date_value(command))

    def test_transform_ecs_command_unquoted(self):
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_transform(task_id="agg_orders", transform_name="starbake.agg_orders")
        command = task.overrides["containerOverrides"][0]["command"]
        _assert_unquoted(_scheduled_date_value(command))

    def test_async_ecs_command_unquoted(self):
        """The async branch builds the same overrides inside a TaskGroup —
        covers the matrix "sync/async" row."""
        job = self._make_job({"fargate_async": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        op = dag.get_task("load_customers_wait.load_customers")
        command = op.overrides["containerOverrides"][0]["command"]
        _assert_unquoted(_scheduled_date_value(command))

    def test_explicit_scheduled_date_unquoted(self):
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                scheduled_date="2026-01-01T00:00:00+0000",
            )
        command = task.overrides["containerOverrides"][0]["command"]
        assert _scheduled_date_value(command) == "2026-01-01T00:00:00+0000"
