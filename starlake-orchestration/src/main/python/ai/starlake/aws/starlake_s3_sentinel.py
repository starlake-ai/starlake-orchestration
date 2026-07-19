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

"""Default S3 handlers for the pre-load not-ready sentinel (issue #122).

The ``boto3`` import is LAZY and lives on the default-client path only:
``ai.starlake.aws.__init__`` is imported at DAG-parse time by existing
consumers (the Fargate helper), so the SDK must never be an import-time
requirement — and an injected client (the test seam) must not trigger any
SDK import at all.
"""

from typing import Callable, Optional, Tuple


def s3_sentinel_handlers(client: Optional[object] = None) -> Tuple[Callable[[str], bool], Callable[[str], None]]:
    """Build the ``(exists_fn, delete_fn)`` pair for ``s3://`` sentinel URIs.

    Args:
        client: optional pre-built ``boto3`` S3-client-compatible object
            (``list_objects_v2`` / ``delete_object``).  When provided, NO
            SDK import happens.  When omitted, ``boto3`` is imported lazily
            on first use; a missing SDK raises an actionable error naming
            the extra.

    Existence uses ``list_objects_v2`` with an exact-key match (no
    exception classes needed — ``head_object`` would require botocore error
    handling); deletion uses ``delete_object`` (idempotent in S3).
    """
    state = {'client': client}

    def _client():
        if state['client'] is None:
            try:
                import boto3
            except ImportError as e:
                raise RuntimeError(
                    "boto3 is required to consume an s3:// pre-load "
                    "sentinel — install it with "
                    "'pip install starlake-orchestration[aws]'"
                ) from e
            state['client'] = boto3.client('s3')
        return state['client']

    def _bucket_key(uri: str):
        from ai.starlake.sentinel import parse_uri
        scheme, bucket, key = parse_uri(uri)
        if scheme != 's3':
            raise ValueError(f"not an s3:// sentinel URI: '{uri}'")
        return bucket, key

    def exists(uri: str) -> bool:
        bucket, key = _bucket_key(uri)
        response = _client().list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
        return any(entry.get('Key') == key for entry in response.get('Contents', []))

    def delete(uri: str) -> None:
        bucket, key = _bucket_key(uri)
        _client().delete_object(Bucket=bucket, Key=key)

    return exists, delete
