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
would force every test that touches these helpers to install the full stack.
"""
from enum import Enum
from typing import Optional, Tuple


class SentinelDecision(Enum):
    """Outcome of checking the sentinel after a successful pre-load task."""
    READY = "ready"
    NOT_READY = "not_ready"


def derive_sentinel_path(
    sl_datasets: str,
    domain: str,
    override: Optional[str],
) -> str:
    """Produce the sentinel path to pass to --notReadySentinel and to the sensor.

    The default path includes a ``{{ run_id }}`` Jinja placeholder that Airflow
    resolves at render time, making concurrent DAG runs safe without coordination.

    If ``override`` is provided it wins verbatim, except for a single ``{domain}``
    placeholder that we substitute here (Airflow's Jinja would not, since
    ``domain`` is not in its context). Other placeholders like ``{{ run_id }}``
    pass through unchanged for Airflow to template.
    """
    if override is not None:
        return override.replace("{domain}", domain)
    return f"{sl_datasets.rstrip('/')}/_sl/preload/{domain}/{{{{ run_id }}}}.notready"


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


def decide_from_existence(sentinel_exists: bool) -> SentinelDecision:
    """Map a raw existence check to the semantic decision."""
    return SentinelDecision.NOT_READY if sentinel_exists else SentinelDecision.READY
