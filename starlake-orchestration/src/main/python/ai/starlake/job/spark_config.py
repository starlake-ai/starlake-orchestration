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

from ai.starlake.job.starlake_options import StarlakeOptions

class StarlakeSparkExecutorConfig:
    def __init__(self, memory: str, cores: int, instances: int):
        super().__init__()
        self.memory = memory
        self.cores = cores
        self.instances = instances

    def __config__(self):
        return {
            'spark.executor.memory': self.memory,
            'spark.executor.cores': str(self.cores),
            'spark.executor.instances': str(self.instances)
        }
 
    def __str__(self):
        return str(self.__dict__)

class StarlakeSparkConfig(StarlakeSparkExecutorConfig):
    def __init__(self, memory: str, cores: int, instances: int, cls_options: StarlakeOptions, options: dict, **kwargs):
        super().__init__(
            memory = cls_options.__class__.get_context_var(var_name='spark_executor_memory', default_value='11g', options={} if not options else options) if not memory else memory,
            cores = cls_options.__class__.get_context_var(var_name='spark_executor_cores', default_value=4, options={} if not options else options) if not cores else cores,
            instances = cls_options.__class__.get_context_var(var_name='spark_executor_instances', default_value=1, options={} if not options else options) if not instances else instances
        )
        self.spark_properties = kwargs

    def __config__(self):
        return dict(
            super().__config__(),
            **self.spark_properties
        )
