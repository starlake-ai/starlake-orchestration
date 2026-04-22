"""Integration-ish checks for CloudRunJobCompletionSensor's sentinel contract.

These tests require Airflow to be importable, so they skip gracefully in bare
envs. They pin two invariants the unit tests cannot cover:

1. ``sentinel_path`` is listed in ``template_fields``. Without this, Airflow
   would pass the raw string ``gs://.../{{ run_id }}.notready`` to the poke()
   method instead of the substituted per-run path — concurrent DAG runs would
   collide on one sentinel object. A future refactor that drops or overwrites
   ``template_fields`` would silently break concurrency; this test guards it.

2. The constructor actually stores a ``sentinel_path`` kwarg. Without this,
   ``sl_job`` could silently drop it and the sensor would behave like the
   legacy code (no sentinel), which would be a quiet regression.
"""
import pytest

airflow = pytest.importorskip("airflow")

from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import CloudRunJobCompletionSensor


def test_sentinel_path_is_a_template_field():
    # Concurrent-safety depends on Airflow rendering {{ run_id }} at render time.
    assert "sentinel_path" in CloudRunJobCompletionSensor.template_fields


def test_sensor_constructor_stores_sentinel_path():
    sensor = CloudRunJobCompletionSensor(
        task_id="t",
        dataset=None,
        source=None,
        source_task_id="t_upstream",
        sentinel_path="gs://b/p/{{ run_id }}.notready",
    )
    assert sensor.sentinel_path == "gs://b/p/{{ run_id }}.notready"


def test_sensor_defaults_sentinel_path_to_none():
    # Legacy call sites (no sentinel_path kwarg) must still work — feature is opt-in.
    sensor = CloudRunJobCompletionSensor(
        task_id="t",
        dataset=None,
        source=None,
        source_task_id="t_upstream",
    )
    assert sensor.sentinel_path is None
