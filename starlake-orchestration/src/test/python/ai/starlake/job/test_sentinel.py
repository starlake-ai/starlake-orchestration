"""Unit tests for ai.starlake.job.sentinel — pure helpers, no cloud I/O."""
import pytest

from ai.starlake.sentinel import (
    derive_sentinel_path,
    parse_gcs_uri,
    SentinelDecision,
    decide_from_existence,
)


# --- derive_sentinel_path ----------------------------------------------------

def test_derive_sentinel_path_default_template():
    path = derive_sentinel_path(
        sl_datasets="gs://my-bucket/datasets",
        domain="sales",
        override=None,
    )
    assert path == "gs://my-bucket/datasets/_sl/preload/sales/{{ run_id }}.notready"


def test_derive_sentinel_path_strips_trailing_slash_from_sl_datasets():
    path = derive_sentinel_path(
        sl_datasets="gs://my-bucket/datasets/",
        domain="sales",
        override=None,
    )
    assert path == "gs://my-bucket/datasets/_sl/preload/sales/{{ run_id }}.notready"


def test_derive_sentinel_path_honors_override():
    path = derive_sentinel_path(
        sl_datasets="gs://my-bucket/datasets",
        domain="sales",
        override="gs://other-bucket/my-sentinels/{{ run_id }}.flag",
    )
    assert path == "gs://other-bucket/my-sentinels/{{ run_id }}.flag"


def test_derive_sentinel_path_substitutes_domain_in_override():
    path = derive_sentinel_path(
        sl_datasets="gs://my-bucket/datasets",
        domain="sales",
        override="gs://b/{domain}/x.flag",
    )
    assert path == "gs://b/sales/x.flag"


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


# --- decide_from_existence ---------------------------------------------------

def test_decide_from_existence_present():
    assert decide_from_existence(True) is SentinelDecision.NOT_READY


def test_decide_from_existence_absent():
    assert decide_from_existence(False) is SentinelDecision.READY
