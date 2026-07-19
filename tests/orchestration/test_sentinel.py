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

"""Story 6.12 (issue #122) — pure ``ai.starlake.sentinel`` module.

No orchestrator imports here: the module is deliberately import-light and
its cloud I/O is dependency-injected.  The gcp/aws handler factories are
exercised with injected fake clients, asserting NO SDK import happens on
the injected path (the lazy import lives on the default-client path only).
"""

from __future__ import annotations

import sys

import pytest

from ai.starlake.sentinel import (
    SENTINEL_OPTION,
    SENTINEL_SCOPE_TOKEN,
    consume_sentinel,
    default_sentinel_handlers,
    local_sentinel_handlers,
    parse_uri,
    require_scheme,
    resolve_sentinel_path,
    sanitize_scope,
    substitute_scope,
)

HOSTILE_PARTS = [
    "run id with spaces",
    "run'quote\"double",
    "$(rm -rf /)",
    "run;id`whoami`",
    "unicode-éøñ-日本",
    "back\\slash|pipe&amp",
]


# ---------------------------------------------------------------------------
# 1. parse_uri
# ---------------------------------------------------------------------------

class TestParseUri:

    def test_gs(self):
        assert parse_uri("gs://bucket/a/b.notready") == ("gs", "bucket", "a/b.notready")

    def test_s3(self):
        assert parse_uri("s3://bucket/a/b.notready") == ("s3", "bucket", "a/b.notready")

    def test_file(self):
        assert parse_uri("file:///tmp/a/b") == ("file", "", "/tmp/a/b")

    def test_scheme_less(self):
        assert parse_uri("/tmp/a/b") == ("", "", "/tmp/a/b")

    @pytest.mark.parametrize("bad", ["gs://bucket", "gs://bucket/", "s3://", "s3://bucket"])
    def test_bucket_without_key_raises(self, bad):
        with pytest.raises(ValueError):
            parse_uri(bad)

    @pytest.mark.parametrize("bad", ["hdfs://nn/path", "wasb://x/y", "http://x/y"])
    def test_unknown_scheme_raises(self, bad):
        with pytest.raises(ValueError) as exc_info:
            parse_uri(bad)
        assert bad in str(exc_info.value)

    def test_non_string_raises(self):
        with pytest.raises(ValueError):
            parse_uri(42)


# ---------------------------------------------------------------------------
# 2. resolve_sentinel_path — canonical option contract
# ---------------------------------------------------------------------------

class TestResolveSentinelPath:

    def test_absent_option_is_off(self):
        assert resolve_sentinel_path({}, "starbake") is None
        assert resolve_sentinel_path(None, "starbake") is None

    @pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
    def test_blank_option_is_off(self, blank):
        assert resolve_sentinel_path({SENTINEL_OPTION: blank}, "starbake") is None

    @pytest.mark.parametrize("bad", [42, True, ["gs://b/p"], {"a": 1}])
    def test_non_string_raises(self, bad):
        with pytest.raises(ValueError) as exc_info:
            resolve_sentinel_path({SENTINEL_OPTION: bad}, "starbake")
        assert SENTINEL_OPTION in str(exc_info.value)

    def test_gs_prefix_resolves_with_domain_and_token(self):
        path = resolve_sentinel_path({SENTINEL_OPTION: "gs://bucket/sentinels/"}, "starbake")
        assert path == f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    def test_s3_prefix_resolves(self):
        path = resolve_sentinel_path({SENTINEL_OPTION: "s3://bucket/sent"}, "starbake")
        assert path == f"s3://bucket/sent/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    def test_local_absolute_prefix_resolves(self):
        path = resolve_sentinel_path({SENTINEL_OPTION: "/tmp/sentinels"}, "starbake")
        assert path == f"/tmp/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    def test_file_absolute_prefix_resolves(self):
        path = resolve_sentinel_path({SENTINEL_OPTION: "file:///tmp/sentinels"}, "starbake")
        assert path == f"file:///tmp/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    @pytest.mark.parametrize("prefix", ["gs://bucket", "gs://bucket/", "s3://bucket"])
    def test_bucket_root_prefix_is_valid(self, prefix):
        """Review finding — a bucket-root parent is legitimate: the resolved
        path always appends <domain>/<scope>.notready below it."""
        path = resolve_sentinel_path({SENTINEL_OPTION: prefix}, "starbake")
        assert path == f"{prefix.rstrip('/')}/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    @pytest.mark.parametrize("relative", ["relative/dir", "file://relative/dir", "./x"])
    def test_relative_local_prefix_raises(self, relative):
        with pytest.raises(ValueError) as exc_info:
            resolve_sentinel_path({SENTINEL_OPTION: relative}, "starbake")
        assert "ABSOLUTE" in str(exc_info.value)

    def test_unknown_scheme_prefix_raises(self):
        with pytest.raises(ValueError):
            resolve_sentinel_path({SENTINEL_OPTION: "hdfs://nn/sentinels"}, "starbake")


# ---------------------------------------------------------------------------
# 3. sanitize_scope / substitute_scope — hostile inputs
# ---------------------------------------------------------------------------

class TestScopeSanitization:

    def test_clean_scope_is_untouched(self):
        assert sanitize_scope("dag_id__scheduled__2026-07-18T00:00:00+00:00") == \
            "dag_id__scheduled__2026-07-18T00:00:00+00:00"

    @pytest.mark.parametrize("hostile", HOSTILE_PARTS)
    def test_hostile_scope_is_neutralized(self, hostile):
        sanitized = sanitize_scope(hostile)
        # every char is whitelisted after sanitization...
        import re
        assert re.fullmatch(r'[A-Za-z0-9_.+:=-]*', sanitized)
        # ...and shell metacharacters are gone
        for char in " '\"$`;|&\\()":
            assert char not in sanitized

    def test_substitute_scope_joins_and_sanitizes(self):
        text = f"gs://b/d/{SENTINEL_SCOPE_TOKEN}.notready"
        got = substitute_scope(text, "my_dag", "manual run 'x'")
        assert got == "gs://b/d/my_dag__manual_run__x_.notready"
        assert SENTINEL_SCOPE_TOKEN not in got

    def test_substitute_scope_none_passthrough(self):
        assert substitute_scope(None, "a", "b") is None

    def test_substitute_scope_no_token_is_identity(self):
        assert substitute_scope("gs://b/plain", "a", "b") == "gs://b/plain"


# ---------------------------------------------------------------------------
# 4. require_scheme — engine-aware gate
# ---------------------------------------------------------------------------

class TestRequireScheme:

    def test_allowed_scheme_returns_it(self):
        assert require_scheme("gs://b/k", ("gs",), "cloud_run") == "gs"
        assert require_scheme("/tmp/x", ("", "file"), "shell") == ""
        assert require_scheme("file:///tmp/x", ("", "file"), "shell") == "file"

    def test_mismatch_names_engine_and_scheme(self):
        with pytest.raises(ValueError) as exc_info:
            require_scheme("gs://b/k", ("", "file"), "shell")
        message = str(exc_info.value)
        assert "shell" in message
        assert "gs" in message

    def test_fargate_rejects_gs(self):
        with pytest.raises(ValueError) as exc_info:
            require_scheme("gs://b/k", ("s3",), "fargate")
        assert "fargate" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 5. consume_sentinel — consume-then-signal with injected callables
# ---------------------------------------------------------------------------

class TestConsumeSentinel:

    def test_none_path_is_ready(self):
        assert consume_sentinel(None, None, None) is True

    def test_absent_sentinel_is_ready_and_not_deleted(self):
        deleted = []
        assert consume_sentinel("gs://b/k", lambda p: False, deleted.append) is True
        assert deleted == []

    def test_gcs_delete_tolerates_already_gone_marker(self):
        """Review finding — exists/delete race or manual cleanup counts as
        consumed on GCS too (matches local/S3 idempotence)."""
        from ai.starlake.gcp import gcs_sentinel_handlers

        class _GoneBlob:
            def exists(self):
                return True

            def delete(self):
                error = type("NotFound", (Exception,), {})()
                raise error

        class _GoneClient:
            def bucket(self, name):
                blob = _GoneBlob()
                return type("B", (), {"blob": staticmethod(lambda key: blob)})()

        exists, delete = gcs_sentinel_handlers(client=_GoneClient())
        # delete of an already-gone marker must not raise
        delete("gs://bucket/d/scope.notready")

    def test_present_sentinel_is_deleted_first_then_not_ready(self):
        calls = []
        ready = consume_sentinel(
            "gs://b/k",
            lambda p: calls.append(("exists", p)) or True,
            lambda p: calls.append(("delete", p)),
        )
        assert ready is False
        assert calls == [("exists", "gs://b/k"), ("delete", "gs://b/k")]


# ---------------------------------------------------------------------------
# 6. Local handlers end-to-end on tmp_path
# ---------------------------------------------------------------------------

class TestLocalHandlers:

    def test_local_roundtrip(self, tmp_path):
        exists, delete = local_sentinel_handlers()
        sentinel = tmp_path / "starbake" / "scope.notready"
        sentinel.parent.mkdir()
        assert exists(str(sentinel)) is False
        sentinel.touch()
        assert exists(str(sentinel)) is True
        delete(str(sentinel))
        assert not sentinel.exists()
        # delete is tolerant of a missing file (already consumed)
        delete(str(sentinel))

    def test_file_scheme_stripped(self, tmp_path):
        exists, delete = local_sentinel_handlers()
        sentinel = tmp_path / "scope.notready"
        sentinel.touch()
        assert exists(f"file://{sentinel}") is True
        delete(f"file://{sentinel}")
        assert not sentinel.exists()

    def test_remote_uri_rejected(self):
        exists, _ = local_sentinel_handlers()
        with pytest.raises(ValueError):
            exists("gs://bucket/key")

    def test_consume_sentinel_with_local_handlers(self, tmp_path):
        exists, delete = local_sentinel_handlers()
        sentinel = tmp_path / "scope.notready"
        sentinel.touch()
        assert consume_sentinel(str(sentinel), exists, delete) is False
        assert not sentinel.exists()
        assert consume_sentinel(str(sentinel), exists, delete) is True


# ---------------------------------------------------------------------------
# 7. Dispatcher
# ---------------------------------------------------------------------------

class TestDefaultHandlerDispatch:

    def test_local_dispatch(self, tmp_path):
        sentinel = tmp_path / "scope.notready"
        sentinel.touch()
        exists, delete = default_sentinel_handlers(str(sentinel))
        assert exists(str(sentinel)) is True
        delete(str(sentinel))
        assert not sentinel.exists()

    def test_unknown_scheme_raises(self):
        with pytest.raises(ValueError):
            default_sentinel_handlers("hdfs://nn/key")

    def test_gs_dispatch_routes_to_gcp_package(self):
        exists, delete = default_sentinel_handlers("gs://bucket/key")
        import ai.starlake.gcp.starlake_gcs_sentinel as gcs_module
        assert exists.__module__ == gcs_module.__name__
        assert delete.__module__ == gcs_module.__name__

    def test_s3_dispatch_routes_to_aws_package(self):
        exists, delete = default_sentinel_handlers("s3://bucket/key")
        import ai.starlake.aws.starlake_s3_sentinel as s3_module
        assert exists.__module__ == s3_module.__name__
        assert delete.__module__ == s3_module.__name__


# ---------------------------------------------------------------------------
# 8. GCS handler factory with an injected fake client (no SDK import)
# ---------------------------------------------------------------------------

class _FakeBlob:
    def __init__(self, store, key):
        self._store = store
        self._key = key

    def exists(self):
        return self._key in self._store

    def delete(self):
        self._store.remove(self._key)


class _FakeGcsBucket:
    def __init__(self, store):
        self._store = store

    def blob(self, key):
        return _FakeBlob(self._store, key)


class _FakeGcsClient:
    def __init__(self, store):
        self._store = store

    def bucket(self, name):
        assert name == "bucket"
        return _FakeGcsBucket(self._store)


class TestGcsHandlerFactory:

    def test_injected_client_roundtrip_without_sdk_import(self):
        from ai.starlake.gcp import gcs_sentinel_handlers

        store = {"d/scope.notready"}
        exists, delete = gcs_sentinel_handlers(client=_FakeGcsClient(store))

        before = set(sys.modules)
        assert exists("gs://bucket/d/scope.notready") is True
        delete("gs://bucket/d/scope.notready")
        assert exists("gs://bucket/d/scope.notready") is False
        # the injected-client path must not import the SDK at all
        newly_imported = set(sys.modules) - before
        assert not any(name.startswith("google") for name in newly_imported)

    def test_non_gs_uri_rejected(self):
        from ai.starlake.gcp import gcs_sentinel_handlers
        exists, _ = gcs_sentinel_handlers(client=_FakeGcsClient(set()))
        with pytest.raises(ValueError):
            exists("s3://bucket/key")

    def test_missing_sdk_error_names_the_extra(self, monkeypatch):
        from ai.starlake.gcp import gcs_sentinel_handlers
        # force the lazy import to fail even if the SDK is installed locally
        import builtins
        real_import = builtins.__import__

        def _no_google(name, *args, **kwargs):
            if name.startswith("google"):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _no_google)
        monkeypatch.delitem(sys.modules, "google.cloud.storage", raising=False)
        monkeypatch.delitem(sys.modules, "google.cloud", raising=False)
        exists, _ = gcs_sentinel_handlers()
        with pytest.raises(RuntimeError) as exc_info:
            exists("gs://bucket/key")
        assert "starlake-orchestration[gcp]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 9. S3 handler factory with an injected fake client (no SDK import)
# ---------------------------------------------------------------------------

class _FakeS3Client:
    def __init__(self, store):
        self._store = store

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        assert Bucket == "bucket"
        contents = [{"Key": key} for key in self._store if key.startswith(Prefix)]
        return {"Contents": contents[:MaxKeys]} if contents else {}

    def delete_object(self, Bucket, Key):
        assert Bucket == "bucket"
        self._store.discard(Key)


class TestS3HandlerFactory:

    def test_injected_client_roundtrip_without_sdk_import(self):
        from ai.starlake.aws import s3_sentinel_handlers

        store = {"d/scope.notready"}
        exists, delete = s3_sentinel_handlers(client=_FakeS3Client(store))

        before = set(sys.modules)
        assert exists("s3://bucket/d/scope.notready") is True
        delete("s3://bucket/d/scope.notready")
        assert exists("s3://bucket/d/scope.notready") is False
        newly_imported = set(sys.modules) - before
        assert not any(name.startswith("boto") for name in newly_imported)

    def test_prefix_match_is_not_existence(self):
        """list_objects_v2 returns prefix matches — only the EXACT key counts."""
        from ai.starlake.aws import s3_sentinel_handlers
        store = {"d/scope.notready.other"}
        exists, _ = s3_sentinel_handlers(client=_FakeS3Client(store))
        assert exists("s3://bucket/d/scope.notready") is False

    def test_non_s3_uri_rejected(self):
        from ai.starlake.aws import s3_sentinel_handlers
        exists, _ = s3_sentinel_handlers(client=_FakeS3Client(set()))
        with pytest.raises(ValueError):
            exists("gs://bucket/key")

    def test_missing_sdk_error_names_the_extra(self, monkeypatch):
        from ai.starlake.aws import s3_sentinel_handlers
        import builtins
        real_import = builtins.__import__

        def _no_boto(name, *args, **kwargs):
            if name.startswith("boto"):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _no_boto)
        monkeypatch.delitem(sys.modules, "boto3", raising=False)
        exists, _ = s3_sentinel_handlers()
        with pytest.raises(RuntimeError) as exc_info:
            exists("s3://bucket/key")
        assert "starlake-orchestration[aws]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 10. Package import hygiene — the gcp/aws __init__ modules must stay
#     importable without any SDK (they load at DAG-parse time)
# ---------------------------------------------------------------------------

class TestPackageImportHygiene:

    def test_sentinel_module_is_import_light(self):
        import ai.starlake.sentinel as sentinel_module
        import ast
        tree = ast.parse(open(sentinel_module.__file__).read())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        # no orchestrator/job import anywhere, no SDK import anywhere (the
        # gs/s3 branches import ONLY the ai.starlake.gcp/aws factories, whose
        # own SDK imports are lazy)
        assert not any(name.startswith("ai.starlake.job") for name in imported)
        assert not any(name.startswith(("google", "boto")) for name in imported)

    def test_handler_modules_have_no_top_level_sdk_import(self):
        import ai.starlake.gcp.starlake_gcs_sentinel as gcs_module
        import ai.starlake.aws.starlake_s3_sentinel as s3_module
        import ast
        for module in (gcs_module, s3_module):
            tree = ast.parse(open(module.__file__).read())
            top_level_imports = [
                name.name if isinstance(node, ast.Import) else node.module
                for node in tree.body
                if isinstance(node, (ast.Import, ast.ImportFrom))
                for name in (node.names if isinstance(node, ast.Import) else [node])
            ]
            assert not any(
                str(name).startswith(("google", "boto")) for name in top_level_imports
            ), f"top-level SDK import in {module.__name__}: {top_level_imports}"
