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
OPTIONS_MERGE = StarlakeParameters.OPTIONS_MERGE_PARAMETER.value
UNION = {"policy": "union", "separator": "|"}


def _event(sections: dict, merge: dict = None):
    extra = {OPTIONS: sections}
    if merge is not None:
        extra[OPTIONS_MERGE] = merge
    return types.SimpleNamespace(extra=extra)


def _dag_run(conf: dict = None):
    return types.SimpleNamespace(conf=conf or {})


def _found(uri: str, sections: dict = None, merge: dict = None, event_id: int = 7) -> dict:
    """An entry of the XCom the start task pushes for an event it accepted for a
    declared dataset that did not trigger the run."""
    return {"uri": uri, "event_id": event_id, OPTIONS: sections, OPTIONS_MERGE: merge}


class _TaskInstance:
    """A task instance whose xcom_pull answers the start task's
    found-options XCom and records every pull."""

    def __init__(self, found):
        self.found = found
        self.pulls = []

    def xcom_pull(self, task_ids=None, key=None, **kwargs):
        self.pulls.append({"task_ids": task_ids, "key": key})
        from ai.starlake.airflow.starlake_airflow_job import SL_FOUND_OPTIONS_XCOM_KEY
        if task_ids == "start" and key == SL_FOUND_OPTIONS_XCOM_KEY:
            return self.found
        return None


def _ti(found=None):
    return _TaskInstance(found)


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
# sl_options_from_events — keys declared union by their producer (issue #158)
# ------------------------------------------------------------------

class TestSlOptionsUnionMerge:

    def _union_event(self, value: str, declaration: dict = UNION):
        return _event({"all": {"fd": value}}, {"all": {"fd": declaration}})

    def test_coalesced_union_values_merge(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [self._union_event("A|B"), self._union_event("B|C")]}
        assert sl_options_from_events(events, _dag_run(), "d.t") == "fd=A|B|C"

    def test_the_order_of_events_does_not_change_the_result(self):
        from ai.starlake.airflow import sl_options_from_events
        first = {"uri1": [self._union_event("2026-11-01|2026-11-02"), self._union_event("2026-11-02|2026-11-03")]}
        second = {"uri1": [self._union_event("2026-11-02|2026-11-03"), self._union_event("2026-11-01|2026-11-02")]}
        expected = "fd=2026-11-01|2026-11-02|2026-11-03"
        assert sl_options_from_events(first, _dag_run(), None) == expected
        assert sl_options_from_events(second, _dag_run(), None) == expected

    def test_union_across_dataset_uris(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [self._union_event("A|B")], "uri2": [_event({"all": {"fd": "C"}})]}
        assert sl_options_from_events(events, _dag_run(), None) == "fd=A|B|C"

    def test_the_same_events_without_declaration_still_fail_loud(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri1": [_event({"all": {"fd": "A|B"}}), _event({"all": {"fd": "B|C"}})]}
        with pytest.raises(AirflowException, match="Conflicting values"):
            sl_options_from_events(events, _dag_run(), None)

    def test_different_separators_fail_loud_naming_the_key(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri1": [self._union_event("A|B"), self._union_event("B;C", {"policy": "union", "separator": ";"})]}
        with pytest.raises(AirflowException, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            sl_options_from_events(events, _dag_run(), None)

    def test_different_policies_fail_loud_naming_the_key(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri1": [self._union_event("A|B"), self._union_event("A|B", {"policy": "strict"})]}
        with pytest.raises(AirflowException, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            sl_options_from_events(events, _dag_run(), None)

    def test_dag_run_conf_overrides_the_union(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [self._union_event("A|B"), self._union_event("B|C")]}
        run = _dag_run({OPTIONS: {"all": {"fd": "D"}}})
        assert sl_options_from_events(events, run, None) == "fd=D"

    def test_a_single_event_is_normalized(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri1": [self._union_event("B||A|B")]}
        assert sl_options_from_events(events, _dag_run(), None) == "fd=A|B"

    def test_conflict_names_the_uri_of_an_asset_key(self):
        """Airflow 3 keys the triggering events by Asset objects: the error
        names their URI."""
        import dataclasses
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException

        @dataclasses.dataclass(frozen=True)
        class _Asset:
            uri: str

        events = {_Asset("gs://bucket/orders"): [_event({"all": {"fd": "A"}}), _event({"all": {"fd": "B"}})]}
        with pytest.raises(AirflowException, match=r"\(gs://bucket/orders, gs://bucket/orders\)"):
            sl_options_from_events(events, _dag_run(), None)

    def test_producer_extra_carries_the_declaration(self, airflow_job):
        extra = {OPTIONS: {"all": {"fd": "{{ ti.xcom_pull(task_ids='claim') }}"}}, OPTIONS_MERGE: {"all": {"fd": UNION}}}
        task = airflow_job.sl_load(task_id=None, domain="d", table="t", extra=extra)
        assert task.extra[OPTIONS_MERGE] == {"all": {"fd": UNION}}
        # the outlet events are published with this extra (pre_execute re-syncs it after rendering)
        assert task.outlets and all(outlet.extra[OPTIONS_MERGE] == {"all": {"fd": UNION}} for outlet in task.outlets)


# ------------------------------------------------------------------
# sl_options_from_events — events accepted by the start task for the
# datasets that did not trigger the run
# ------------------------------------------------------------------

class TestSlOptionsFromFoundEvents:
    """Under the ANY triggering strategy, the run triggered by the last
    dataset to arrive passes the start task thanks to the earlier events it
    finds for the other datasets: the run variables of those events reach the
    tasks through the start task's XCom, merged with the triggering events
    under the same rules."""

    # the run triggered by B (loaded on 2 December); the start task
    # accepted the event of A (loaded on 1 December)
    TRIGGERING = {"uri_b": [_event({"all": {"b_fd": "2026-12-02"}})]}

    def test_found_events_options_are_passed_through(self):
        from ai.starlake.airflow import sl_options_from_events
        from ai.starlake.airflow.starlake_airflow_job import SL_FOUND_OPTIONS_XCOM_KEY
        ti = _ti([_found("uri_a", {"all": {"a_fd": "2026-12-01"}})])
        rendered = sl_options_from_events(self.TRIGGERING, _dag_run(), "d.t", ti)
        assert set(rendered.split(",")) == {"b_fd=2026-12-02", "a_fd=2026-12-01"}
        assert ti.pulls == [{"task_ids": "start", "key": SL_FOUND_OPTIONS_XCOM_KEY}]

    def test_found_events_options_alone(self):
        """A triggering event without run variables: the accepted event's
        variables are the whole fragment."""
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri_b": [types.SimpleNamespace(extra={})]}
        ti = _ti([_found("uri_a", {"all": {"a_fd": "2026-12-01"}})])
        assert sl_options_from_events(events, _dag_run(), "d.t", ti) == "a_fd=2026-12-01"

    def test_task_specific_section_of_a_found_event(self):
        from ai.starlake.airflow import sl_options_from_events
        ti = _ti([_found("uri_a", {"all": {"k": "1"}, "d.t": {"k": "2"}})])
        assert sl_options_from_events({}, _dag_run(), "d.t", ti) == "k=2"
        assert sl_options_from_events({}, _dag_run(), "d.other", ti) == "k=1"

    def test_same_value_in_a_triggering_and_a_found_event_is_not_a_conflict(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri_b": [_event({"all": {"fd": "2026-12-01"}})]}
        ti = _ti([_found("uri_a", {"all": {"fd": "2026-12-01"}})])
        assert sl_options_from_events(events, _dag_run(), None, ti) == "fd=2026-12-01"

    ACCEPTED_RECOVERY = (
        "The triggering events and the events accepted by the start task carry different run variables "
        "— run the DAG manually, passing dag_run.conf['sl_options'] (a manual run skips the check)."
    )
    COALESCED_RECOVERY = "re-trigger the runs one by one, passing dag_run.conf['sl_options']"

    def test_conflict_with_a_triggering_event_fails_loud(self):
        """The same key carried with different values by a triggering event and
        an accepted one stops the run, naming both. Nothing was coalesced and
        clearing the run would accept the same event again: the recovery is a
        manual run."""
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri_b": [_event({"all": {"fd": "2026-12-02"}})]}
        ti = _ti([_found("uri_a", {"all": {"fd": "2026-12-01"}}, event_id=42)])
        with pytest.raises(AirflowException) as raised:
            sl_options_from_events(events, _dag_run(), None, ti)
        message = str(raised.value)
        assert "Conflicting values for sl_options['all']['fd']" in message
        assert "across triggering dataset events and the events accepted by the start task" in message
        assert "(uri_b, uri_a (event 42 accepted by the start task))" in message
        assert self.ACCEPTED_RECOVERY in message
        assert self.COALESCED_RECOVERY not in message
        # the core still suggests the union declaration
        assert "policy 'union'" in message

    def test_conflict_between_two_found_events_fails_loud(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        ti = _ti([_found("uri_a", {"all": {"fd": "A"}}), _found("uri_c", {"all": {"fd": "B"}})])
        with pytest.raises(AirflowException, match="Conflicting values") as raised:
            sl_options_from_events({}, _dag_run(), None, ti)
        assert self.ACCEPTED_RECOVERY in str(raised.value)
        assert self.COALESCED_RECOVERY not in str(raised.value)

    def test_a_conflict_among_the_coalesced_triggering_events_keeps_its_message(self):
        """An accepted event takes part without conflicting: the coalesced
        triggering events that conflict on their own keep their origin and
        their recovery."""
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri_b": [_event({"all": {"fd": "2026-12-02"}}), _event({"all": {"fd": "2026-12-03"}})]}
        ti = _ti([_found("uri_a", {"all": {"a_fd": "2026-12-01"}})])
        with pytest.raises(AirflowException) as raised:
            sl_options_from_events(events, _dag_run(), None, ti)
        message = str(raised.value)
        assert "across triggering dataset events (uri_b, uri_b)" in message
        assert self.COALESCED_RECOVERY in message
        assert self.ACCEPTED_RECOVERY not in message
        assert "accepted by the start task" not in message

    def test_union_declared_by_the_triggering_event(self):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri_b": [_event({"all": {"fd": "A|B"}}, {"all": {"fd": UNION}})]}
        ti = _ti([_found("uri_a", {"all": {"fd": "B|C"}})])
        assert sl_options_from_events(events, _dag_run(), None, ti) == "fd=A|B|C"

    def test_union_declared_by_the_found_event(self):
        """The declaration describes the value's type: carried by the accepted
        event only, it applies to the triggering event's value too."""
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri_b": [_event({"all": {"fd": "C|A"}})]}
        ti = _ti([_found("uri_a", {"all": {"fd": "B"}}, {"all": {"fd": UNION}})])
        assert sl_options_from_events(events, _dag_run(), None, ti) == "fd=A|B|C"

    def test_conflicting_declarations_across_triggering_and_found_events_fail_loud(self):
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        events = {"uri_b": [_event({"all": {"fd": "A"}}, {"all": {"fd": UNION}})]}
        ti = _ti([_found("uri_a", {"all": {"fd": "B"}}, {"all": {"fd": {"policy": "union", "separator": ";"}}})])
        with pytest.raises(AirflowException, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            sl_options_from_events(events, _dag_run(), None, ti)

    def test_a_malformed_declaration_of_a_found_event_fails_loud(self):
        """A malformed sl_options_merge carried by an accepted event fails the
        rendering, naming the event, as it does for a triggering event."""
        from ai.starlake.airflow import sl_options_from_events
        from airflow.exceptions import AirflowException
        ti = _ti([_found("uri_a", {"all": {"fd": "A"}}, "not-a-dict")])
        with pytest.raises(AirflowException, match=r"Invalid sl_options_merge carried by uri_a \(event 7 accepted by the start task\)"):
            sl_options_from_events(self.TRIGGERING, _dag_run(), None, ti)

    def test_dag_run_conf_overrides_the_found_events(self):
        from ai.starlake.airflow import sl_options_from_events
        ti = _ti([_found("uri_a", {"all": {"a_fd": "2026-12-01"}})])
        run = _dag_run({OPTIONS: {"all": {"a_fd": "2026-12-03"}}})
        rendered = sl_options_from_events(self.TRIGGERING, run, None, ti)
        assert set(rendered.split(",")) == {"b_fd=2026-12-02", "a_fd=2026-12-03"}

    @pytest.mark.parametrize("pulled", [None, []], ids=["no-xcom", "empty"])
    def test_no_found_events_leaves_the_result_unchanged(self, pulled):
        from ai.starlake.airflow import sl_options_from_events
        events = {"uri_b": [_event({"all": {"fd": "2026-12-02"}}), _event({"all": {"fd": "2026-12-03"}})]}
        assert sl_options_from_events(self.TRIGGERING, _dag_run(), "d.t", _ti(pulled)) == "b_fd=2026-12-02"
        assert sl_options_from_events({}, _dag_run(), "d.t", _ti(pulled)) == "sl_options_applied=0"
        # the coalesced-events error keeps its origin when nothing was found
        from airflow.exceptions import AirflowException
        with pytest.raises(AirflowException, match=r"across triggering dataset events \(uri_b, uri_b\)"):
            sl_options_from_events(events, _dag_run(), None, _ti(pulled))

    def test_without_a_task_instance_nothing_is_pulled(self):
        """The three-argument form (a custom template, a direct call) keeps its
        result."""
        from ai.starlake.airflow import sl_options_from_events
        assert sl_options_from_events(self.TRIGGERING, _dag_run(), "d.t") == "b_fd=2026-12-02"
        assert sl_options_from_events(self.TRIGGERING, _dag_run(), "d.t", None) == "b_fd=2026-12-02"


# ------------------------------------------------------------------
# sl_transform — runtime options fragment appended to --options
# ------------------------------------------------------------------

class TestSlTransformInjection:

    def test_transform_command_carries_the_macro_fragment(self, airflow_job):
        from ai.starlake.airflow.compat import supports_assets
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options=None)
        cmd = task.bash_command
        events_key = "triggering_asset_events" if supports_assets() else "triggering_dataset_events"
        fragment = "{{sl_options_from_events(%s, dag_run, 'd.t', ti)}}" % events_key
        assert fragment in cmd
        # the fragment must be part of the (quoted) --options value, appended last
        options_value = cmd.split("--options", 1)[1].strip()
        assert options_value.startswith('"') and options_value.endswith('"')
        assert options_value.strip('"').endswith(fragment)

    def test_static_transform_options_precede_the_fragment(self, airflow_job):
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options="k=v")
        options_value = task.bash_command.split("--options", 1)[1].strip()
        assert options_value.index("k=v") < options_value.index("sl_options_from_events")

    def test_transform_fragment_uses_asset_events_key_on_airflow_3(self, airflow_job, monkeypatch):
        """Airflow 3 renamed the template context key to triggering_asset_events;
        the fragment must be built with the version-appropriate name (the DAG's
        StrictUndefined default would otherwise fail the render)."""
        import airflow
        monkeypatch.setattr(airflow, "__version__", "3.0.2")
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options=None)
        assert "sl_options_from_events(triggering_asset_events, dag_run, 'd.t', ti)" in task.bash_command
        assert "triggering_dataset_events" not in task.bash_command

    def test_transform_fragment_renders_the_found_events_options(self, airflow_job):
        """The fragment hands the task instance to the macro, which pulls the
        run variables of the events accepted by the start task: rendered with
        the context Airflow gives the task, a variable carried only by such an
        event reaches the command."""
        import jinja2
        from ai.starlake.airflow import sl_options_from_events
        from ai.starlake.airflow.compat import supports_assets
        task = airflow_job.sl_transform(task_id=None, transform_name="d.t", transform_options=None)
        cmd = task.bash_command
        begin = cmd.index("{{sl_options_from_events(")
        fragment = cmd[begin:cmd.index(")}}", begin) + len(")}}")]
        events_key = "triggering_asset_events" if supports_assets() else "triggering_dataset_events"
        rendered = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(fragment).render(**{
            "sl_options_from_events": sl_options_from_events,
            events_key: {"uri_b": [_event({"all": {"b_fd": "2026-12-02"}})]},
            "dag_run": _dag_run(),
            "ti": _ti([_found("uri_a", {"all": {"a_fd": "2026-12-01"}})]),
        })
        assert set(rendered.split(",")) == {"b_fd=2026-12-02", "a_fd=2026-12-01"}


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
