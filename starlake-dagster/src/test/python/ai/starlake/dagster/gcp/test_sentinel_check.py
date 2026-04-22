"""Dagster-side sentinel integration uses the same ai.starlake.sentinel.consume_sentinel
helper as the Airflow sensor. These tests exercise the contract the Dagster op
relies on: present → delete + caller raises, absent → proceed.

We deliberately re-cover the key paths here (rather than just trusting the
shared-helper tests) because the Dagster op passes its own GCS-adapter
closures, and a broken adapter would silently break the Dagster side only.
"""
from unittest.mock import MagicMock
import pytest

from ai.starlake.sentinel import consume_sentinel


def test_dagster_closure_style_adapter_absent():
    # Dagster passes google.cloud.storage.Client-style closures. Simulate one.
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = False
    client.bucket.return_value.blob.return_value = blob

    result = consume_sentinel(
        sentinel_path="gs://my-bucket/path/sentinel.flag",
        exists_fn=lambda b, o: client.bucket(b).blob(o).exists(),
        delete_fn=lambda b, o: client.bucket(b).blob(o).delete(),
    )
    assert result is True
    blob.delete.assert_not_called()


def test_dagster_closure_style_adapter_present():
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = True
    client.bucket.return_value.blob.return_value = blob

    result = consume_sentinel(
        sentinel_path="gs://my-bucket/path/sentinel.flag",
        exists_fn=lambda b, o: client.bucket(b).blob(o).exists(),
        delete_fn=lambda b, o: client.bucket(b).blob(o).delete(),
    )
    assert result is False
    blob.delete.assert_called_once()


def test_dagster_no_sentinel_skips_all_gcs_calls():
    client = MagicMock()
    # exists_fn and delete_fn must NEVER be invoked when the feature is off.
    exists_fn = MagicMock(side_effect=AssertionError("should not be called"))
    delete_fn = MagicMock(side_effect=AssertionError("should not be called"))
    assert consume_sentinel(
        sentinel_path=None,
        exists_fn=exists_fn,
        delete_fn=delete_fn,
    ) is True
    exists_fn.assert_not_called()
    delete_fn.assert_not_called()
