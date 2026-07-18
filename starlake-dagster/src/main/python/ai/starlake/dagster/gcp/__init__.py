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

from .starlake_dagster_cloud_run_job import StarlakeDagsterCloudRunJob

# issue #108: the Cloud Run variant submits executions through the gcloud CLI
# and has no dagster-gcp dependency — only the Dataproc variant needs
# dagster_gcp. Keep this package (and star-imports of it) usable when
# dagster-gcp is absent: StarlakeDagsterDataprocJob is then a placeholder
# that raises an informative error at instantiation, and __all__ omits the
# dataproc module so `from ai.starlake.dagster.gcp import *` does not trigger
# its import (Dataproc users install the 'gcp' extra).
try:
    from .starlake_dagster_dataproc_job import StarlakeDagsterDataprocJob
    __all__ = ['starlake_dagster_cloud_run_job', 'starlake_dagster_dataproc_job']
except ModuleNotFoundError as e:
    if e.name != 'dagster_gcp':
        raise
    __all__ = ['starlake_dagster_cloud_run_job']
    _dataproc_import_error = e

    class StarlakeDagsterDataprocJob:
        """Placeholder: the real class requires the optional dagster-gcp dependency."""
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError(
                "StarlakeDagsterDataprocJob requires the optional 'dagster-gcp' "
                "dependency — install it (e.g. pip install starlake-dagster[gcp]) "
                f"to use the Dataproc execution environment: {_dataproc_import_error}"
            ) from _dataproc_import_error
