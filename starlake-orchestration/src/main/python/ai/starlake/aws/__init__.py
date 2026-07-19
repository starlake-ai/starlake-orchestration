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

__all__ = ['starlake_fargate_helper', 'starlake_s3_sentinel', 's3_sentinel_handlers']

from .starlake_fargate_helper import StarlakeFargateHelper

# issue #122 — default S3 sentinel handlers; the module keeps its boto3
# import LAZY, so this export adds no import-time SDK requirement (this
# __init__ loads at DAG-parse time)
from .starlake_s3_sentinel import s3_sentinel_handlers