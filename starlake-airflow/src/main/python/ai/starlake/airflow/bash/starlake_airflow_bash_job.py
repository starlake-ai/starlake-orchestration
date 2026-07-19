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

from typing import Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from ai.starlake.airflow.compat import BaseOperator, BashOperator, BashSensor, PythonOperator

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
        import os
        env_vars = dict()
        # Add all env vars if sl_include_env_vars is * or _
        if self.sl_included_env_vars == ['*'] or self.sl_included_env_vars == ['_']:
            env_vars = os.environ.copy()
        else:
            # Add the SL_ environment variables from the os environment variables
            for key in self.sl_included_env_vars:
                if key in os.environ:
                    env_vars[key] = os.environ[key]
        return env_vars

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SHELL

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
        found = False

        env = {**self.sl_os_env_vars.copy(), **self.sl_env_vars.copy()} # Copy the current sl env variables

        # explicit --scheduledDate override (e.g. a per-run file date pulled from an
        # XCom template) — popped unconditionally: BaseOperator would reject the kwarg
        scheduled_date = kwargs.pop('scheduled_date', None)

        if task_type is not None and (task_type == TaskType.LOAD or task_type == TaskType.TRANSFORM):
            arguments = [] if not arguments else arguments
            params: dict = kwargs.get('params', dict())
            cron = params.get('cron_expr', params.get('cron', None))
            params.update({'cron': cron})
            kwargs.update({'params': params})
            tmp_arguments = []
            tmp_arguments.append("--scheduledDate")
            if scheduled_date:
                tmp_arguments.append(f"\'{scheduled_date}\'")
            else:
                tmp_arguments.append("\'{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts)).strftime('%Y-%m-%dT%H:%M:%S%z')}}\'")
            command = arguments.pop(0)
            arguments = [command] + tmp_arguments + arguments

        for index, arg in enumerate(arguments):
            if arg == "--options" and arguments.__len__() > index + 1:
                opts = arguments[index+1]
                if opts.strip().__len__() > 0:
                    temp = self.sl_env_vars.copy() # Copy the current sl env variables
                    temp.update({
                        key: value
                        for opt in opts.split(",")
                        if "=" in opt  # Only process valid key=value pairs
                        for key, value in [opt.split("=")]
                    })
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(temp.items())])
                    for opt in opts.split(","):
                        if "=" not in opt:
                            options += f",{opt}"
                else:
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) # Add/overwrite with sl env variables
                # double quotes so values containing spaces survive bash word
                # splitting; the do_xcom_push wrapper is a flat script (story
                # 6.4, issue #95) so these quotes are parsed exactly once
                arguments[index+1] = f'"{options}"'
                found = True
                break

        if not found:
            arguments.append("--options")
            options = ",".join([f"{key}={value}" for key, value in self.sl_env_vars.items()]) # Add/overwrite with sl env variables
            arguments.append(f'"{options}"')

        preload = False
        if task_type and task_type==TaskType.PRELOAD:
            preload = True

        # story 6.2 (issue #86) — sensor-mode kwargs are popped unconditionally:
        # BaseOperator would reject the unknown kwargs
        pre_load_sensor = bool(kwargs.pop('pre_load_sensor', False))
        pre_load_poke_interval = kwargs.pop('pre_load_poke_interval', 300)
        pre_load_timeout = kwargs.pop('pre_load_timeout', 3600)
        pre_load_sensor_soft_fail = bool(kwargs.pop('pre_load_sensor_soft_fail', False))

        # story 6.12 (issue #122) — not-ready sentinel: popped unconditionally
        # (BaseOperator would reject the kwarg); only PRELOAD consumes it
        sentinel_path = kwargs.pop('sentinel_path', None)
        sentinel_test = sentinel_rm = None
        if preload and sentinel_path:
            from ai.starlake.sentinel import SENTINEL_SCOPE_TOKEN, require_scheme
            # engine-aware scheme gate: the shell wrapper can only test local
            # files — a gs:// path here would silently read as a permanent
            # false READY ([ -f gs://... ] is always absent)
            scheme = require_scheme(sentinel_path, ('', 'file'), 'shell')
            local_path = sentinel_path[len('file://'):] if scheme == 'file' else sentinel_path
            # composition-time token surgery: the literal scope token becomes
            # the ${SL_SENTINEL_SCOPE_SAFE} shell variable — computed by the
            # wrapper's tr whitelist from the SL_SENTINEL_SCOPE env VALUE
            # (Jinja renders the ids into data, never into shell code).
            # Double-quoted: prefixes may contain spaces (#51).
            shell_ref = '"' + local_path.replace(SENTINEL_SCOPE_TOKEN, '${SL_SENTINEL_SCOPE_SAFE}') + '"'
            for index, arg in enumerate(arguments):
                if arg == "--notReadySentinel" and len(arguments) > index + 1:
                    arguments[index+1] = '"' + arguments[index+1].replace(SENTINEL_SCOPE_TOKEN, '${SL_SENTINEL_SCOPE_SAFE}') + '"'
                    break
            env['SL_SENTINEL_SCOPE'] = StarlakeAirflowJob._SL_SENTINEL_SCOPE_JINJA
            sentinel_test = f"[ -f {shell_ref} ]"
            sentinel_rm = f"rm -f {shell_ref}" + ' || { echo "sentinel rm failed"; exit 1; }'

        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if preload and pre_load_sensor:
            # a retried sensor restarts the whole poke window — default retries
            # to 0; an explicit retries kwarg or an explicitly provided retries
            # option still wins (story 6.1 precedence contract)
            if 'retries' not in kwargs:
                if 'retries' in (self.options or {}):
                    kwargs['retries'] = self.retries # already resolved from the option by core
                else:
                    kwargs['retries'] = 0
            kwargs.setdefault('mode', 'reschedule')
            # BashSensor has no cwd parameter (unlike BashOperator) — cd into
            # sl_root (double-quoted: paths may contain spaces, cf. #51). The
            # RAW command is used (no xcom echo-wrapper) so the true exit code
            # drives the poke: non-zero → poke again, 0 → done.
            sensor_command = f'cd "{self.sl_root}" && {command}'
            if sentinel_test is not None:
                # story 6.12 — sentinel sensor mode: closed {0,1,2} contract
                # (0 = ready/done, 2 = not ready/poke again, 1 = real failure)
                # with retry_exit_code=2 so CLI exit codes can never
                # masquerade as poke-again (matches the in-repo cloud_run
                # gcloud sensor convention)
                sensor_command = StarlakeAirflowJob._sl_sentinel_sensor_command(
                    sensor_command, sentinel_test, sentinel_rm
                )
                # the closed {0,1,2} contract OWNS this code — a caller
                # override would invert real-failure vs poke-again
                kwargs['retry_exit_code'] = 2
            return StarlakePreloadBashSensor(
                task_id=task_id,
                dataset=dataset,
                source=self.source,
                bash_command=sensor_command,
                env=env,
                poke_interval=pre_load_poke_interval,
                timeout=pre_load_timeout,
                soft_fail=pre_load_sensor_soft_fail,
                **kwargs
            )

        if preload and sentinel_test is not None:
            # story 6.12 — sentinel one-shot: the exit-code swallow is REMOVED
            # (a crashed CLI fails the task; sentinel precedence over
            # retry_on_failure, decision 4); on exit 0 the wrapper consumes
            # the marker and echoes the skip_or_start verdict (0/1)
            command = StarlakeAirflowJob._sl_sentinel_wrapped_command(
                command, sentinel_test, sentinel_rm
            )
        elif kwargs.get('do_xcom_push', False):
            # story 6.3 (issue #92) — shared wrapper builder: preload swallows
            # the exit code (XCom-gated via skip_or_start), every other task
            # type keeps the active `exit $return_code` trailer
            command = self.__class__._sl_xcom_wrapped_command(command, preload)
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

class StarlakePreloadBashSensor(StarlakeDatasetMixin, BashSensor):
    """Starlake Preload Bash Sensor (story 6.2, issue #86).

    Pokes the raw ``starlake preload`` command until it exits 0 within the
    wall-clock ``timeout``.  ``retry_exit_code`` stays ``None`` so ANY
    non-zero exit pokes again (a genuinely broken CLI invocation therefore
    pokes until timeout instead of failing fast — same behavior class as any
    bash sensor).

    ``execute`` returns ``True`` so the ``do_xcom_push=True`` forced by
    ``StarlakeAirflowJob.sl_pre_load`` records a truthy ``return_value`` on
    success and the downstream ``skip_or_start`` ShortCircuitOperator
    proceeds; on timeout (soft-fail skip or hard fail) no XCom exists,
    ``f_skip_or_start`` pulls ``None`` and the downstream loads are skipped.
    """
    def __init__(
            self,
            task_id: str,
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            bash_command: str,
            sentinel_scope_in_environ: bool = False,
            **kwargs
        ):
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            bash_command=bash_command,
            **kwargs
        )
        # story 6.12 (issue #122) — gcloud waiting path: BashSensor has no
        # append_env and the gcloud command needs the INHERITED process
        # environment (env=None), so the run scope cannot travel as a
        # templated env-dict value there. When set, poke() computes the
        # sanitized scope PYTHON-side (ai.starlake.sentinel.sanitize_scope)
        # and exports SL_SENTINEL_SCOPE_SAFE around the subprocess — the
        # wrapper then references ${SL_SENTINEL_SCOPE_SAFE} without its own
        # tr line (values stay data; never rendered into shell code).
        # NOTE: mutates os.environ around super().poke() (restored in a
        # finally) — safe under Airflow's one-task-per-process execution;
        # not safe if two sensors of DIFFERENT runs ever poke concurrently
        # in one process (no supported executor does).
        self._sentinel_scope_in_environ = sentinel_scope_in_environ

    def poke(self, context):
        if self._sentinel_scope_in_environ:
            import os
            from ai.starlake.sentinel import sanitize_scope
            dag_id, run_id = StarlakeAirflowJob._sl_sentinel_scope_parts(context)
            scope = sanitize_scope(f"{dag_id}__{run_id}")
            previous = os.environ.get('SL_SENTINEL_SCOPE_SAFE', None)
            os.environ['SL_SENTINEL_SCOPE_SAFE'] = scope
            try:
                return super().poke(context)
            finally:
                if previous is None:
                    os.environ.pop('SL_SENTINEL_SCOPE_SAFE', None)
                else:
                    os.environ['SL_SENTINEL_SCOPE_SAFE'] = previous
        return super().poke(context)

    def execute(self, context):
        super().execute(context)
        return True
