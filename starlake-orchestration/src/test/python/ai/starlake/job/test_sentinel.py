"""Unit tests for ai.starlake.sentinel — pure helpers, no cloud I/O."""
import pytest

from ai.starlake.sentinel import (
    parse_gcs_uri,
    resolve_sentinel_path,
    substitute_airflow_placeholders,
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


# --- substitute_airflow_placeholders -----------------------------------------

def test_substitute_airflow_placeholders_none_is_passthrough():
    assert substitute_airflow_placeholders(None, "some-run-id") is None


def test_substitute_airflow_placeholders_substitutes_run_id():
    result = substitute_airflow_placeholders(
        "gs://b/_sl/preload/sales/{{ run_id }}.notready",
        "scheduled__2026-04-22T06-00-00",
    )
    assert result == "gs://b/_sl/preload/sales/scheduled__2026-04-22T06-00-00.notready"


def test_substitute_airflow_placeholders_no_placeholder_returns_verbatim():
    # If the path doesn't reference {{ run_id }}, it's unchanged.
    result = substitute_airflow_placeholders(
        "gs://b/fixed/path.notready",
        "some-run-id",
    )
    assert result == "gs://b/fixed/path.notready"


def test_substitute_airflow_placeholders_multiple_occurrences():
    # All occurrences substituted, in case a template reuses {{ run_id }}.
    result = substitute_airflow_placeholders(
        "gs://b/{{ run_id }}/log-{{ run_id }}.notready",
        "xyz",
    )
    assert result == "gs://b/xyz/log-xyz.notready"
