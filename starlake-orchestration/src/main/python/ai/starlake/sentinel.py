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

"""Pure helpers for the opt-in pre-load 'not ready' sentinel feature.

No cloud I/O here — these helpers are unit-testable in isolation. I/O lives
in the Airflow sensor and the Dagster op, which call these functions.

Located at ``ai.starlake.sentinel`` (not under ``ai.starlake.job``) to avoid
pulling in the full orchestration dependency chain at test-collection time:
``ai.starlake.job.__init__`` eagerly imports croniter / pytz / etc., which
would force every test that touches these pure helpers to install the full stack.
"""
from typing import Callable, Optional, Tuple


def resolve_sentinel_path(options: dict, domain: str) -> Optional[str]:
    """Return the sentinel path if the feature is enabled for this DAG, else None.

    Feature is opt-in via a single DagInfo option:
      pre_load_not_ready_sentinel_path: <path>
    If this option is set to a non-empty value, the feature is ON and the path
    is used. If missing or empty, the feature is OFF and this function returns
    None (caller skips all sentinel wiring).

    A ``{domain}`` placeholder in the path is substituted here. Other placeholders
    — most importantly ``{{ run_id }}`` — pass through unchanged so Airflow can
    template them at task-render time, which is what makes concurrent DAG runs
    safe without coordination.
    """
    template = options.get('pre_load_not_ready_sentinel_path', '')
    if not template:
        return None
    return template.replace('{domain}', domain)


def substitute_airflow_placeholders(value: Optional[str], run_id: str) -> Optional[str]:
    """Substitute Airflow-style Jinja placeholders at op/task execution time.

    Airflow templates fields like ``{{ run_id }}`` automatically at task-render
    time. Dagster has no equivalent Jinja layer, so without help a Dagster op
    would see the literal string ``{{ run_id }}`` and write/check a sentinel at
    a fixed path — defeating the per-run uniqueness that keeps concurrent DAG
    runs from racing.

    This helper lets the Dagster op apply the same substitution at execution
    time using ``context.run_id``. Both runners then produce the same concrete
    path from the same user-facing placeholder syntax, so the README's
    ``gs://.../{{ run_id }}.notready`` template works uniformly.
    """
    if value is None:
        return None
    return value.replace("{{ run_id }}", run_id)


def consume_sentinel(
    sentinel_path: Optional[str],
    exists_fn: Optional[Callable[[str, str], bool]],
    delete_fn: Optional[Callable[[str, str], None]],
) -> bool:
    """Check-and-consume the 'not ready' sentinel after a successful pre-load.

    Returns True if the load may proceed (no sentinel configured, or sentinel
    is absent). Returns False if the sentinel is present — caller should then
    raise their platform-appropriate exception (``AirflowException`` for the
    Airflow sensor, ``dagster.Failure`` for the Dagster op) so the orchestrator's
    retry machinery can convert "not ready" into "wait and re-run".

    The helper deletes the sentinel on detection so the next retry starts fresh.

    ``exists_fn`` and ``delete_fn`` are injected (bucket, object_name) → ... callables,
    keeping this module platform-neutral (no GCS SDK import, no AirflowException
    import) and trivially unit-testable.
    """
    if sentinel_path is None:
        return True
    bucket, obj = parse_gcs_uri(sentinel_path)
    if exists_fn(bucket, obj):
        delete_fn(bucket, obj)
        return False
    return True


def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    """Split 'gs://bucket/object/path' into (bucket, object_path). Raises on malformed input."""
    if not uri.startswith("gs://"):
        raise ValueError(f"not a gs:// URI: {uri}")
    without_scheme = uri[len("gs://"):]
    if "/" not in without_scheme:
        raise ValueError(f"missing object name in gs:// URI: {uri}")
    bucket, obj = without_scheme.split("/", 1)
    if not obj:
        raise ValueError(f"missing object name in gs:// URI: {uri}")
    return bucket, obj
