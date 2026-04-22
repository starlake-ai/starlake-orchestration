"""Tests for consume_sentinel — shared across Airflow and Dagster.

Placed under starlake-airflow for historical reasons (the feature grew out of
Cloud Run + Airflow usage), but the helper itself is platform-neutral and
tested here without importing Airflow.
"""
from unittest.mock import MagicMock
import pytest

from ai.starlake.sentinel import consume_sentinel


def test_no_path_configured_returns_true():
    # Feature off → caller should just proceed. No GCS calls made.
    assert consume_sentinel(
        sentinel_path=None,
        exists_fn=None,
        delete_fn=None,
    ) is True


def test_sentinel_absent_returns_true_without_delete():
    exists_fn = MagicMock(return_value=False)
    delete_fn = MagicMock()
    result = consume_sentinel(
        sentinel_path="gs://b/path/flag",
        exists_fn=exists_fn,
        delete_fn=delete_fn,
    )
    assert result is True
    exists_fn.assert_called_once_with("b", "path/flag")
    delete_fn.assert_not_called()


def test_sentinel_present_deletes_and_returns_false():
    exists_fn = MagicMock(return_value=True)
    delete_fn = MagicMock()
    result = consume_sentinel(
        sentinel_path="gs://b/path/flag",
        exists_fn=exists_fn,
        delete_fn=delete_fn,
    )
    assert result is False
    delete_fn.assert_called_once_with("b", "path/flag")


def test_sentinel_non_gs_uri_raises_configuration_error():
    # Misconfigured sentinel path surfaces clearly, not silently treated as ready.
    with pytest.raises(ValueError, match="not a gs://"):
        consume_sentinel(
            sentinel_path="s3://b/flag",
            exists_fn=MagicMock(),
            delete_fn=MagicMock(),
        )
