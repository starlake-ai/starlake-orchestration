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

"""Pure helpers for the opt-in pre-load "not ready" sentinel (issue #122).

Starlake CLI >= 1.5.15 supports ``preload --notReadySentinel <uri>``: on a
"not ready" decision (IMPORTED/PENDING empty, ACK file missing) the CLI
touches a ZERO-BYTE marker at exactly the given URI and exits 0; on "ready"
no marker is written; on a genuine crash the process still exits non-zero
and never writes the marker.  The marker turns the lossy exit-code channel
into a deterministic verdict: exit 0 + marker = not ready (keep waiting /
skip), exit 0 + no marker = ready (proceed), non-zero = real failure (fail
now).

This module is deliberately import-light (stdlib + typing only — no
``ai.starlake.job`` import, no cloud SDK import at module level) so the
helpers are unit-testable without any orchestrator installed.  Cloud I/O is
dependency-injected: ``consume_sentinel`` takes ``exists_fn``/``delete_fn``
callables over the FULL sentinel URI; ``default_sentinel_handlers``
dispatches per scheme to the handler factories shipped in
``ai.starlake.gcp`` / ``ai.starlake.aws`` (lazy SDK imports) or to the
stdlib local handlers below.

The best-effort-write caveat (CLI design): a failed marker write is logged
and swallowed CLI-side — "exit 0 + no sentinel" can, rarely, mean "not
ready but the marker write failed".  For IMPORTED/PENDING this yields a
no-op load (the pending area is empty by definition of not-ready); for ACK
the pending area may hold real files, so a lost write can trigger a
PREMATURE load of un-acked data.
"""

import re

from typing import Callable, Optional, Tuple

#: Option key carrying the sentinel parent prefix (feature off when absent
#: or blank).
SENTINEL_OPTION = 'pre_load_not_ready_sentinel_path'

#: Literal scope token embedded in the resolved sentinel path.  Deliberately
#: NOT a ``{{ ... }}`` Jinja placeholder: an Airflow template pass over a
#: command string can never render it away or empty it.  Each orchestrator
#: replaces it at RUN time with the sanitized run scope
#: (``<dag_id>__<run_id>`` on Airflow, ``<job_name>__<run_id>`` on Dagster).
SENTINEL_SCOPE_TOKEN = "__SL_SENTINEL_SCOPE__"

#: Whitelist used by :func:`sanitize_scope` — any other character maps to
#: ``_``.  A manual-trigger ``run_id`` is user-controlled free text (spaces,
#: quotes, ``$(...)``): the sanitizer is what keeps the scope safe as a
#: path segment AND as a shell word.
_SCOPE_WHITELIST = re.compile(r'[^A-Za-z0-9_.+:=-]')

_KNOWN_SCHEMES = ('gs', 's3', 'file')


def parse_uri(uri: str) -> Tuple[str, str, str]:
    """Split a sentinel URI into ``(scheme, bucket_or_root, key)``.

    - ``gs://bucket/key`` → ``('gs', 'bucket', 'key')``
    - ``s3://bucket/key`` → ``('s3', 'bucket', 'key')``
    - ``file:///abs/path`` → ``('file', '', '/abs/path')``
    - ``/abs/path`` (scheme-less) → ``('', '', '/abs/path')``

    Raises:
        ValueError: on any other scheme, or on a bucket URI without a key.
    """
    if not isinstance(uri, str):
        raise ValueError(f"sentinel URI must be a string, got {type(uri).__name__}")
    for scheme in ('gs', 's3'):
        prefix = scheme + '://'
        if uri.startswith(prefix):
            remainder = uri[len(prefix):]
            bucket, _, key = remainder.partition('/')
            if not bucket or not key:
                raise ValueError(
                    f"invalid sentinel URI '{uri}' — expected {scheme}://<bucket>/<key>"
                )
            return scheme, bucket, key
    if uri.startswith('file://'):
        return 'file', '', uri[len('file://'):]
    if '://' in uri:
        raise ValueError(
            f"unsupported sentinel URI scheme in '{uri}' — supported: "
            f"gs://, s3://, file:// or a scheme-less absolute path"
        )
    return '', '', uri


def resolve_sentinel_path(options: Optional[dict], domain: str) -> Optional[str]:
    """Resolve the sentinel path for a domain from the job options.

    Canonical contract (single source of truth):

    - option absent, or a string that is blank/whitespace-only → ``None``
      (feature OFF — zero change anywhere);
    - non-string value → ``ValueError``;
    - non-blank string → scheme must be ``gs://``, ``s3://``, ``file://`` or
      scheme-less; scheme-less and ``file://`` paths must be ABSOLUTE
      (relative → ``ValueError`` — only the shell wrapper has a defined cwd,
      cloud/Dagster consumers have none); anything else rejected loudly.

    The resolved path is
    ``<prefix.rstrip('/')>/<domain>/__SL_SENTINEL_SCOPE__.notready`` — the
    literal :data:`SENTINEL_SCOPE_TOKEN` is substituted at run time by each
    orchestrator (see :func:`substitute_scope`).
    """
    prefix = (options or {}).get(SENTINEL_OPTION, None)
    if prefix is None:
        return None
    if not isinstance(prefix, str):
        raise ValueError(
            f"invalid value '{prefix}' for option '{SENTINEL_OPTION}' — "
            f"expected a string URI prefix (gs://, s3://, file:// or an "
            f"absolute local path), or absent/blank to disable"
        )
    prefix = prefix.strip()
    if not prefix:
        return None
    # a bucket-ROOT prefix (gs://bucket or s3://bucket/) is a legitimate
    # parent: the resolved path always appends <domain>/<scope>.notready,
    # so the key parse_uri demands materializes below
    root_match = re.fullmatch(r'(gs|s3)://[^/]+/?', prefix)
    if root_match:
        scheme, key = root_match.group(1), ''
    else:
        scheme, _, key = parse_uri(prefix)
    if scheme in ('', 'file') and not key.startswith('/'):
        raise ValueError(
            f"invalid value '{prefix}' for option '{SENTINEL_OPTION}' — "
            f"local/file:// sentinel prefixes must be ABSOLUTE paths "
            f"(no consumer has a defined working directory)"
        )
    return f"{prefix.rstrip('/')}/{domain}/{SENTINEL_SCOPE_TOKEN}.notready"


def sanitize_scope(value: str) -> str:
    """Whitelist-sanitize one scope part: keep ``[A-Za-z0-9_.+:=-]``, map
    any other character (spaces, quotes, ``$``, unicode, ...) to ``_``."""
    return _SCOPE_WHITELIST.sub('_', str(value))


def substitute_scope(text: Optional[str], *parts: str) -> Optional[str]:
    """Replace :data:`SENTINEL_SCOPE_TOKEN` in ``text`` with the sanitized
    scope built by joining ``parts`` with ``__``.

    Used for the polled sentinel paths AND for the CLI-argument rewrites
    (the ``--notReadySentinel`` value embedded in cloud payloads).  Values
    stay data — this is runtime substitution, never Jinja.
    """
    if text is None:
        return None
    scope = sanitize_scope("__".join(str(part) for part in parts))
    return text.replace(SENTINEL_SCOPE_TOKEN, scope)


def require_scheme(uri: str, allowed_schemes: Tuple[str, ...], engine: str) -> str:
    """Engine-aware scheme gate (definition time).

    The core resolver is engine-blind; each consumer validates that the
    sentinel scheme is CONSUMABLE on its engine — without this, a ``gs://``
    prefix on the shell engine would silently test ``[ -f gs://... ]`` →
    always absent → permanent false READY.

    Args:
        uri: the resolved sentinel path.
        allowed_schemes: allowed :func:`parse_uri` schemes (``''`` =
            scheme-less local).
        engine: engine name for the error message.

    Returns:
        The scheme of ``uri``.

    Raises:
        ValueError: when the scheme is not allowed on this engine.
    """
    scheme, _, _ = parse_uri(uri)
    if scheme not in allowed_schemes:
        pretty = ", ".join(
            (s + '://') if s else 'a scheme-less absolute path'
            for s in allowed_schemes
        )
        raise ValueError(
            f"[{engine}] {SENTINEL_OPTION}: unsupported sentinel scheme "
            f"'{scheme or 'local'}' in '{uri}' — this engine can only "
            f"consume {pretty}"
        )
    return scheme


def consume_sentinel(
    path: Optional[str],
    exists_fn: Callable[[str], bool],
    delete_fn: Callable[[str], None],
) -> bool:
    """Check-and-consume the sentinel. ``True`` = READY (proceed).

    Consume-then-signal: when the sentinel is present it is deleted FIRST,
    then ``False`` (NOT READY) is returned — a stale marker can never turn
    a later, genuinely-ready check into a false skip.  The sentinel is
    re-derivable state, not a ledger: if the worker dies between the delete
    and the verdict being recorded, the next check re-runs the CLI, which
    re-evaluates readiness and re-writes the marker if still not ready.

    ``exists_fn`` / ``delete_fn`` take the FULL sentinel URI (already
    scope-substituted) — see :func:`default_sentinel_handlers`.
    """
    if path is None:
        return True
    if exists_fn(path):
        delete_fn(path)
        return False
    return True


# ---------------------------------------------------------------------------
# Default handlers — local (stdlib) here, gs/s3 factories in ai.starlake.gcp
# and ai.starlake.aws (lazy SDK imports)
# ---------------------------------------------------------------------------

def local_sentinel_handlers() -> Tuple[Callable[[str], bool], Callable[[str], None]]:
    """``(exists_fn, delete_fn)`` for ``file://`` / scheme-less local paths
    (stdlib only)."""
    import os

    def _local_path(uri: str) -> str:
        scheme, _, key = parse_uri(uri)
        if scheme not in ('', 'file'):
            raise ValueError(f"not a local sentinel URI: '{uri}'")
        return key

    def exists(uri: str) -> bool:
        return os.path.isfile(_local_path(uri))

    def delete(uri: str) -> None:
        import contextlib
        with contextlib.suppress(FileNotFoundError):
            os.remove(_local_path(uri))

    return exists, delete


def default_sentinel_handlers(uri: str) -> Tuple[Callable[[str], bool], Callable[[str], None]]:
    """Scheme dispatcher for the default sentinel handlers.

    - ``gs://`` → ``ai.starlake.gcp.gcs_sentinel_handlers()`` (google-cloud-storage,
      install with ``pip install starlake-orchestration[gcp]``);
    - ``s3://`` → ``ai.starlake.aws.s3_sentinel_handlers()`` (boto3,
      install with ``pip install starlake-orchestration[aws]``);
    - ``file://`` / scheme-less → :func:`local_sentinel_handlers`.

    Raises loudly on any other scheme.  A missing SDK surfaces from the
    handler factories with an actionable message naming the extra.
    """
    scheme, _, _ = parse_uri(uri)
    if scheme == 'gs':
        from ai.starlake.gcp import gcs_sentinel_handlers
        return gcs_sentinel_handlers()
    if scheme == 's3':
        from ai.starlake.aws import s3_sentinel_handlers
        return s3_sentinel_handlers()
    return local_sentinel_handlers()
