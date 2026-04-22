"""Unit tests for ai.starlake.sentinel — pure helpers, no cloud I/O."""
import pytest

from ai.starlake.sentinel import (
    parse_gcs_uri,
    resolve_sentinel_path,
)


# --- resolve_sentinel_path ---------------------------------------------------

def test_resolve_sentinel_path_missing_option_returns_none():
    assert resolve_sentinel_path(options={}, domain="sales") is None


def test_resolve_sentinel_path_empty_string_returns_none():
    # Airflow options often come through as empty strings when unset.
    assert resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": ""},
        domain="sales",
    ) is None


def test_resolve_sentinel_path_non_empty_returns_template_verbatim():
    # The {{ run_id }} placeholder is preserved for Airflow to template at render time.
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "gs://b/preload/{{ run_id }}.flag"},
        domain="sales",
    )
    assert path == "gs://b/preload/{{ run_id }}.flag"


def test_resolve_sentinel_path_substitutes_domain_placeholder():
    # {domain} is substituted here (not an Airflow Jinja variable).
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "gs://b/{domain}/{{ run_id }}.flag"},
        domain="sales",
    )
    assert path == "gs://b/sales/{{ run_id }}.flag"


def test_resolve_sentinel_path_handles_non_gs_schemes():
    # s3://, hdfs://, file:// — the pure helper doesn't care about scheme;
    # scheme-specific logic belongs to the callers (Airflow sensor, Dagster op).
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "s3://b/{domain}/sentinel.flag"},
        domain="sales",
    )
    assert path == "s3://b/sales/sentinel.flag"


# --- parse_gcs_uri -----------------------------------------------------------

def test_parse_gcs_uri_basic():
    bucket, obj = parse_gcs_uri("gs://my-bucket/path/to/file.flag")
    assert bucket == "my-bucket"
    assert obj == "path/to/file.flag"


def test_parse_gcs_uri_nested_path():
    bucket, obj = parse_gcs_uri("gs://b/a/b/c/d/e.txt")
    assert bucket == "b"
    assert obj == "a/b/c/d/e.txt"


def test_parse_gcs_uri_rejects_non_gs_scheme():
    with pytest.raises(ValueError, match="not a gs:// URI"):
        parse_gcs_uri("s3://bucket/key")


def test_parse_gcs_uri_rejects_bucket_only():
    with pytest.raises(ValueError, match="missing object name"):
        parse_gcs_uri("gs://bucket")
