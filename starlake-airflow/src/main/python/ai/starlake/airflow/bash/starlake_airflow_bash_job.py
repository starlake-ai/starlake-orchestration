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

import os

from typing import Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from airflow.sdk.bases.operator import BaseOperator

from airflow.providers.standard.operators.bash import BashOperator

from airflow.providers.standard.operators.python import PythonOperator

def wrap_bash_for_xcom(bash_command, exit_on_failure=False):
    """Wrap a bash command to capture return code and push to XCom.

    Args:
        bash_command: The bash command to wrap.
        exit_on_failure: If True, exit with non-zero return code on failure.
    """
    escaped = bash_command.replace("'", '"')
    exit_block = """
            if [ $return_code -ne 0 ]; then
                exit $return_code
            fi""" if exit_on_failure else ""
    return f"""
            set -e
            bash -c '
            {escaped}
            return_code=$?

            # Push the return code to XCom
            echo $return_code
{exit_block}
            '
            """

class StarlakeAirflowBashJob(StarlakeAirflowJob):
    """Airflow Starlake Bash Job."""
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.__sl_included_env_vars: list = str(__class__.get_context_var(var_name='sl_include_env_vars', default_value='GOOGLE_APPLICATION_CREDENTIALS,AWS_KEY_ID,AWS_SECRET_KEY', options=self.options)).split(',')

    @property
    def sl_included_env_vars(self) -> list:
        """Returns the list of os environment variables to include.

        Returns:
            list: The list of os environment variables to include.
        """
        return self.__sl_included_env_vars

    @property
    def sl_os_env_vars(self) -> dict:
        """Returns the os environment variables to use.

        Returns:
            dict: The os environment variables to use.
        """
        if self.sl_included_env_vars in (['*'], ['_']):
            return os.environ.copy()
        return {key: os.environ[key] for key in self.sl_included_env_vars if key in os.environ}

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SHELL

    def _merge_options(self, arguments: list) -> list:
        """Merge sl_env_vars into the --options argument.

        If --options exists in arguments, merges sl_env_vars into it.
        Otherwise, appends --options with all sl_env_vars.
        """
        for index, arg in enumerate(arguments):
            if arg == "--options" and len(arguments) > index + 1:
                opts = arguments[index + 1]
                if opts.strip():
                    temp = self.sl_env_vars.copy()
                    temp.update({
                        key: value
                        for opt in opts.split(",")
                        if "=" in opt
                        for key, value in [opt.split("=")]
                    })
                    options = ",".join(f"{key}={value}" for key, value in temp.items())
                    for opt in opts.split(","):
                        if "=" not in opt:
                            options += f",{opt}"
                else:
                    options = ",".join(f"{key}={value}" for key, value in self.sl_env_vars.items())
                arguments[index + 1] = options
                return arguments

        arguments.append("--options")
        arguments.append(",".join(f"{key}={value}" for key, value in self.sl_env_vars.items()))
        return arguments

    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

        Returns:
            BaseOperator: The Airflow task.
        """
        env = {**self.sl_os_env_vars, **self.sl_env_vars}

        arguments = self._inject_scheduled_date(arguments, task_type, kwargs)
        arguments = self._merge_options(arguments)

        preload = task_type == TaskType.PRELOAD

        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if kwargs.get('do_xcom_push', False):
            command = wrap_bash_for_xcom(command, exit_on_failure=not preload)
        return StarlakeBashOperator(
            task_id=task_id,
            dataset=dataset,
            source=self.source,
            bash_command=command,
            cwd=self.sl_root,
            env=env,
            **kwargs
        )

class StarlakePythonOperator(StarlakeDatasetMixin, PythonOperator):
    """Starlake Python Operator."""
    def __init__(
            self, 
            task_id: str, 
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            python_callable, 
            **kwargs
        ) -> None:
        super().__init__(
            task_id=task_id, 
            dataset=dataset, 
            source=source, 
            python_callable=python_callable, 
            **kwargs
        )

class StarlakeBashOperator(StarlakeDatasetMixin, BashOperator):
    """Starlake Bash Operator."""
    def __init__(
            self, 
            task_id: str, 
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            bash_command: str, 
            **kwargs
        ):
        super().__init__(
            task_id=task_id, 
            dataset=dataset, 
            source=source, 
            bash_command=bash_command, 
            **kwargs
        )
