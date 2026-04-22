"""Unit tests for ai.starlake.sentinel — pure helpers, no cloud I/O."""
import pytest

from ai.starlake.sentinel import (
    parse_gcs_uri,
    resolve_sentinel_path,
    substitute_airflow_placeholders,
)


# --- resolve_sentinel_path ---------------------------------------------------
#
# Contract: user supplies only a prefix. The helper appends "<domain>/{{ run_id }}.notready"
# automatically so users can't forget domain-scoping or per-run uniqueness.

def test_resolve_sentinel_path_missing_option_returns_none():
    assert resolve_sentinel_path(options={}, domain="sales") is None


def test_resolve_sentinel_path_empty_string_returns_none():
    # Airflow options often come through as empty strings when unset.
    assert resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": ""},
        domain="sales",
    ) is None


def test_resolve_sentinel_path_appends_domain_and_run_id_to_prefix():
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "gs://my-bucket/sentinels"},
        domain="sales",
    )
    assert path == "gs://my-bucket/sentinels/sales/{{ run_id }}.notready"


def test_resolve_sentinel_path_strips_trailing_slash_from_prefix():
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "gs://my-bucket/sentinels/"},
        domain="sales",
    )
    assert path == "gs://my-bucket/sentinels/sales/{{ run_id }}.notready"


def test_resolve_sentinel_path_handles_non_gs_schemes():
    # s3://, hdfs://, file:// — resolve_sentinel_path is scheme-agnostic.
    # Scheme-specific I/O happens in the Airflow sensor / Dagster op.
    path = resolve_sentinel_path(
        options={"pre_load_not_ready_sentinel_path": "s3://my-bucket/sentinels"},
        domain="sales",
    )
    assert path == "s3://my-bucket/sentinels/sales/{{ run_id }}.notready"


def test_resolve_sentinel_path_different_domains_produce_different_paths():
    # Domain scoping is automatic — two domains sharing the same prefix still
    # get distinct paths without any user action.
    opts = {"pre_load_not_ready_sentinel_path": "gs://b/s"}
    assert resolve_sentinel_path(opts, "sales") != resolve_sentinel_path(opts, "marketing")


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
