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

from __future__ import annotations

import types

import pytest

from ai.starlake.common import StarlakeParameters

OPTIONS = StarlakeParameters.OPTIONS_PARAMETER.value


def _event(sections: dict):
    return types.SimpleNamespace(extra={OPTIONS: sections})


def _dag_run(conf: dict = None):
    return types.SimpleNamespace(conf=conf or {})


# ------------------------------------------------------------------
# sl_options_from_events — merge across triggering dataset events
# ------------------------------------------------------------------

class TestSlOptionsFromEvents:

    def test_sentinel_when_nothing_applies(self):
        from ai.starlake.airflow import sl_options_from_events
        assert sl_options_from_events({}, _dag_run(), "d.t") == "sl_options_applied=0"
        assert sl_options_from_events(None, None, None) == "sl_options_applied=0"

    def test_all_section_applies_to_any_task(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [_event({"all": {"fd": "2026-07-09"}})]}
        assert sl_options_from_events(events, _dag_run(), "d.t") == "fd=2026-07-09"

    def test_task_specific_section_overrides_all(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [_event({"all": {"k": "1"}, "d.t": {"k": "2"}})]}
        assert sl_options_from_events(events, _dag_run(), "d.t") == "k=2"
        assert sl_options_from_events(events, _dag_run(), "d.other") == "k=1"

    def test_sections_merged_across_events(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {
            "uri1": [_event({"all": {"a_fd": "2026-07-09"}})],
            "uri2": [_event({"all": {"b_fd": "2026-07-10"}})],
        }
        rendered = sl_options_from_events(events, _dag_run(), None)
        assert set(rendered.split(",")) == {"a_fd=2026-07-09", "b_fd=2026-07-10"}

    def test_same_key_same_value_is_not_a_conflict(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [_event({"all": {"fd": "x"}}), _event({"all": {"fd": "x"}})]}
        assert sl_options_from_events(events, _dag_run(), None) == "fd=x"

    def test_coalesced_conflicting_values_fail_loud(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri1": [_event({"all": {"fd": "2026-07-08"}}), _event({"all": {"fd": "2026-07-09"}})]}
        with pytest.raises(AirflowException, match="Conflicting values"):
            sl_options_from_events(events, _dag_run(), None)

    def test_dag_run_conf_overrides_events(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [_event({"all": {"fd": "2026-07-08"}})]}
        run = _dag_run({OPTIONS: {"all": {"fd": "2026-07-10"}}})
        assert sl_options_from_events(events, run, None) == "fd=2026-07-10"


# ------------------------------------------------------------------
# sl_transform — runtime options fragment appended to --options
# ------------------------------------------------------------------

class TestSlTransformInjection:

    def test_transform_command_carries_the_macro_fragment(self, airflow_job):
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options=None)
        cmd = task.bash_command
        fragment = "{{sl_options_from_events(triggering_dataset_events, dag_run, 'd.t')}}"
        assert fragment in cmd
        # the fragment must be part of the (quoted) --options value, appended last
        options_value = cmd.split("--options", 1)[1].strip()
        assert options_value.startswith('"') and options_value.endswith('"')
        assert options_value.strip('"').endswith(fragment)

    def test_static_transform_options_precede_the_fragment(self, airflow_job):
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options="k=v")
        options_value = task.bash_command.split("--options", 1)[1].strip()
        assert options_value.index("k=v") < options_value.index("sl_options_from_events")


# ------------------------------------------------------------------
# sl_load — scheduled_date override and templatable event extra
# ------------------------------------------------------------------

class TestSlLoadProducer:

    def test_scheduled_date_overrides_the_default_template(self, airflow_job):
        xcom = "{{ ti.xcom_pull(task_ids='claim')['file_date'] }}"
        task = airflow_job.sl_load(task_id=None, domain="d", table="t", scheduled_date=xcom)
        assert f"--scheduledDate '{xcom}'" in task.bash_command
        assert "sl_scheduled_date(params.cron" not in task.bash_command.split("--scheduledDate")[1].split("--")[0]

    def test_default_scheduled_date_template_without_override(self, airflow_job):
        task = airflow_job.sl_load(task_id=None, domain="d", table="t")
        assert "--scheduledDate '{{sl_scheduled_date(params.cron" in task.bash_command

    def test_extra_is_popped_and_templatable(self, airflow_job):
        sections = {OPTIONS: {"all": {"fd": "{{ ti.xcom_pull(task_ids='claim')['file_date'] }}"}}}
        # extra must not reach BaseOperator (unexpected kwarg) and must be a template field
        task = airflow_job.sl_load(task_id=None, domain="d", table="t", extra=sections)
        assert "extra" in task.template_fields
        assert task.extra[OPTIONS] == sections[OPTIONS]


# ------------------------------------------------------------------
# sl_import — additional stage options
# ------------------------------------------------------------------

class TestSlImportOptions:

    def test_options_kwarg_folded_into_stage_options(self, airflow_job):
        task = airflow_job.sl_import(task_id=None, domain="d", tables={"t"}, options={"incoming_dir": "gs://bucket/run/x"})
        cmd = task.bash_command
        assert "incoming_dir=gs://bucket/run/x" in cmd
        assert "SL_RUN_MODE=main" in cmd

    def test_options_kwarg_overrides_defaults(self, airflow_job):
        task = airflow_job.sl_import(task_id=None, domain="d", tables={"t"}, options={"SL_LOG_LEVEL": "debug"})
        cmd = task.bash_command
        assert "SL_LOG_LEVEL=debug" in cmd
        assert "SL_LOG_LEVEL=info" not in cmd


# ------------------------------------------------------------------
# Pipeline — macro registered in the DAG's user defined macros
# ------------------------------------------------------------------

class TestMacroRegistration:

    def test_macro_registered_on_the_dag(self, airflow_job):
        from ai.starlake.airflow import AirflowOrchestration, sl_options_from_events
        from ai.starlake.orchestration import StarlakeSchedule
        orch = AirflowOrchestration(job=airflow_job)
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        pipeline = orch.sl_create_pipeline(schedule=schedule)
        with pipeline:
            start = pipeline.start_task()
            end = pipeline.end_task()
            start >> end
        assert pipeline.dag.user_defined_macros.get("sl_options_from_events") is sl_options_from_events
