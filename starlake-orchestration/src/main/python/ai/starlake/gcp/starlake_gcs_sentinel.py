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

"""Default GCS handlers for the pre-load not-ready sentinel (issue #122).

The ``google.cloud.storage`` import is LAZY and lives on the default-client
path only: ``ai.starlake.gcp.__init__`` is imported at DAG-parse time by
existing consumers (Dataproc cluster config), so the SDK must never be an
import-time requirement — and an injected client (the test seam) must not
trigger any SDK import at all.
"""

from typing import Callable, Optional, Tuple


def gcs_sentinel_handlers(client: Optional[object] = None) -> Tuple[Callable[[str], bool], Callable[[str], None]]:
    """Build the ``(exists_fn, delete_fn)`` pair for ``gs://`` sentinel URIs.

    Args:
        client: optional pre-built ``google.cloud.storage.Client``-compatible
            object (``client.bucket(name).blob(key)`` with ``exists()`` /
            ``delete()``).  When provided, NO SDK import happens.  When
            omitted, ``google.cloud.storage`` is imported lazily on first
            use; a missing SDK raises an actionable error naming the extra.
    """
    state = {'client': client}

    def _client():
        if state['client'] is None:
            try:
                from google.cloud import storage
            except ImportError as e:
                raise RuntimeError(
                    "google-cloud-storage is required to consume a gs:// "
                    "pre-load sentinel — install it with "
                    "'pip install starlake-orchestration[gcp]'"
                ) from e
            state['client'] = storage.Client()
        return state['client']

    def _blob(uri: str):
        from ai.starlake.sentinel import parse_uri
        scheme, bucket, key = parse_uri(uri)
        if scheme != 'gs':
            raise ValueError(f"not a gs:// sentinel URI: '{uri}'")
        return _client().bucket(bucket).blob(key)

    def exists(uri: str) -> bool:
        return bool(_blob(uri).exists())

    def delete(uri: str) -> None:
        try:
            _blob(uri).delete()
        except Exception as error:
            # an already-gone marker counts as consumed (exists/delete race,
            # manual cleanup) — matches the local handler's suppressed
            # FileNotFoundError and S3's idempotent delete_object. Matched
            # structurally (name/code) so the injected-client path never
            # needs the SDK's exception types.
            if type(error).__name__ == 'NotFound' or getattr(error, 'code', None) == 404:
                return
            raise

    return exists, delete
