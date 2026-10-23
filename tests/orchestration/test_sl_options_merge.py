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

"""Issue #158 — pure ``ai.starlake.sl_options`` merge of the run variables
carried by several events (shared by starlake-airflow and starlake-dagster)."""

from __future__ import annotations

import itertools

import pytest

from ai.starlake.sl_options import merge_sl_options

UNION = {"policy": "union", "separator": "|"}


def _merge(*carriers):
    return merge_sl_options(carriers, origin="test events", recovery="Recover.")


def _union_event(source, value, declaration=UNION, section="all", key="fd"):
    return (source, {section: {key: value}}, {section: {key: declaration}})


class TestStrictMerge:

    def test_no_carrier(self):
        assert merge_sl_options([], origin="o", recovery="r") == ({}, {})
        assert merge_sl_options(None, origin="o", recovery="r") == ({}, {})

    def test_equal_values_merge(self):
        sections, merge = _merge(("a", {"all": {"k": "x"}}, None), ("b", {"all": {"k": "x"}}, None))
        assert sections == {"all": {"k": "x"}}
        assert merge == {}

    def test_different_values_raise_naming_sources_and_key(self):
        with pytest.raises(ValueError) as e:
            _merge(("a", {"all": {"k": "x"}}, None), ("b", {"all": {"k": "y"}}, None))
        message = str(e.value)
        assert "Conflicting values for sl_options['all']['k'] across test events (a, b)" in message
        assert "'x' != 'y'" in message
        assert "Recover." in message
        assert "sl_options_merge" in message

    def test_explicit_strict_declaration_raises_like_the_default(self):
        strict = {"all": {"k": {"policy": "strict"}}}
        with pytest.raises(ValueError, match="Conflicting values"):
            _merge(("a", {"all": {"k": "x|y"}}, strict), ("b", {"all": {"k": "y|z"}}, None))

    def test_options_payloads_that_are_not_dicts_are_ignored(self):
        sections, merge = _merge(
            ("a", "not-a-dict", None),
            ("b", {"all": "not-a-dict", "d.t": {"k": "v"}}, {}),
        )
        assert sections == {"d.t": {"k": "v"}}
        assert merge == {}

    @pytest.mark.parametrize("payload, error", [
        ("not-a-dict", r"Invalid sl_options_merge carried by a"),
        ({"all": "union"}, r"Invalid sl_options_merge\['all'\] carried by a"),
    ])
    def test_merge_payloads_that_are_not_dicts_raise(self, payload, error):
        """A malformed declaration must not silently leave its keys strict."""
        with pytest.raises(ValueError, match=error):
            _merge(("a", {"all": {"fd": "A"}}, payload))


class TestUnionMerge:

    def test_two_events_give_the_union(self):
        sections, merge = _merge(_union_event("a", "A|B"), _union_event("b", "B|C"))
        assert sections == {"all": {"fd": "A|B|C"}}
        assert merge == {"all": {"fd": UNION}}

    def test_the_order_of_events_does_not_change_the_result(self):
        events = [_union_event("a", "2026-11-02|2026-11-01"), _union_event("b", "2026-11-03|2026-11-02"), _union_event("c", "2026-11-01")]
        results = {_merge(*order)[0]["all"]["fd"] for order in itertools.permutations(events)}
        assert results == {"2026-11-01|2026-11-02|2026-11-03"}

    def test_a_declaration_applies_to_the_events_that_do_not_declare(self):
        sections, _ = _merge(
            ("a", {"all": {"fd": "B|C"}}, None),
            _union_event("b", "A|B"),
        )
        assert sections == {"all": {"fd": "A|B|C"}}

    def test_a_single_event_is_normalized(self):
        sections, _ = _merge(_union_event("a", "C| A||B|A|"))
        assert sections == {"all": {"fd": "A|B|C"}}

    def test_list_values_give_their_items(self):
        sections, _ = _merge(_union_event("a", ["B", "A"]), _union_event("b", "C|B"))
        assert sections == {"all": {"fd": "A|B|C"}}

    def test_empty_and_none_values_give_an_empty_union(self):
        sections, _ = _merge(_union_event("a", ""), _union_event("b", None), _union_event("c", "||"))
        assert sections == {"all": {"fd": ""}}

    def test_the_separator_defaults_to_a_pipe(self):
        sections, merge = _merge(
            _union_event("a", "A|B", {"policy": "union"}),
            _union_event("b", "B|C", UNION),
            _union_event("c", "D", {"policy": "union", "separator": None}),
        )
        assert sections == {"all": {"fd": "A|B|C|D"}}
        assert merge == {"all": {"fd": UNION}}

    def test_a_custom_separator(self):
        semicolon = {"policy": "union", "separator": ";"}
        sections, _ = _merge(_union_event("a", "A;B", semicolon), _union_event("b", "B;C", semicolon))
        assert sections == {"all": {"fd": "A;B;C"}}

    def test_declarations_are_per_section(self):
        sections, _ = _merge(
            ("a", {"all": {"fd": "A"}, "d.t": {"fd": "A"}}, {"d.t": {"fd": UNION}}),
            ("b", {"d.t": {"fd": "B"}}, None),
        )
        assert sections == {"all": {"fd": "A"}, "d.t": {"fd": "A|B"}}
        with pytest.raises(ValueError, match=r"sl_options\['all'\]\['fd'\]"):
            _merge(
                ("a", {"all": {"fd": "A"}}, {"d.t": {"fd": UNION}}),
                ("b", {"all": {"fd": "B"}}, None),
            )

    def test_undeclared_keys_stay_strict_next_to_a_union(self):
        with pytest.raises(ValueError, match=r"sl_options\['all'\]\['scalar'\]"):
            _merge(
                ("a", {"all": {"fd": "A", "scalar": "1"}}, {"all": {"fd": UNION}}),
                ("b", {"all": {"fd": "B", "scalar": "2"}}, None),
            )


class TestDeclarationConflicts:

    def test_different_separators_raise_naming_the_key(self):
        with pytest.raises(ValueError) as e:
            _merge(_union_event("a", "A|B"), _union_event("b", "B;C", {"policy": "union", "separator": ";"}))
        assert "Conflicting merge declarations for sl_options_merge['all']['fd'] across test events (a, b)" in str(e.value)

    def test_different_policies_raise_naming_the_key(self):
        with pytest.raises(ValueError, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            _merge(_union_event("a", "A|B"), _union_event("b", "A|B", {"policy": "strict"}))

    def test_a_declaration_conflict_raises_even_without_values(self):
        with pytest.raises(ValueError, match="Conflicting merge declarations"):
            _merge(("a", None, {"all": {"fd": UNION}}), ("b", None, {"all": {"fd": {"policy": "strict"}}}))

    def test_only_the_declarations_of_merged_keys_are_returned(self):
        sections, merge = _merge(("a", {"all": {"fd": "A"}}, {"all": {"fd": UNION, "gone": UNION}, "d.t": {"x": UNION}}))
        assert sections == {"all": {"fd": "A"}}
        assert merge == {"all": {"fd": UNION}}


class TestSupersededEvents:
    """Events replaced by a later event of the same source (the earlier
    materializations of a Dagster asset)."""

    def test_superseded_values_join_a_union(self):
        sections, merge = merge_sl_options(
            [_union_event("orders#3", "C")], origin="o", recovery="r",
            superseded=[_union_event("orders#1", "A|B"), ("orders#2", {"all": {"fd": "B"}}, None)],
        )
        assert sections == {"all": {"fd": "A|B|C"}}
        assert merge == {"all": {"fd": UNION}}

    def test_superseded_values_of_other_keys_are_ignored(self):
        sections, _ = merge_sl_options(
            [("orders#2", {"all": {"k": "2"}}, None)], origin="o", recovery="r",
            superseded=[("orders#1", {"all": {"k": "1", "only_before": "x"}}, None)],
        )
        assert sections == {"all": {"k": "2"}}

    def test_a_superseded_declaration_counts(self):
        sections, _ = merge_sl_options(
            [("orders#2", {"all": {"fd": "C"}}, None)], origin="o", recovery="r",
            superseded=[_union_event("orders#1", "A")],
        )
        assert sections == {"all": {"fd": "A|C"}}

    def test_a_superseded_declaration_conflict_raises(self):
        with pytest.raises(ValueError, match="Conflicting merge declarations"):
            merge_sl_options(
                [_union_event("orders#2", "C")], origin="o", recovery="r",
                superseded=[_union_event("orders#1", "A", {"policy": "union", "separator": ";"})],
            )

    @pytest.mark.parametrize("declaration, error", [
        ("union", "Invalid merge declaration"),
        ({}, "Invalid merge policy"),
        ({"policy": "concat"}, "Invalid merge policy"),
        ({"policy": "union", "separator": ""}, "Invalid separator"),
        ({"policy": "union", "separator": ","}, "Invalid separator"),
        ({"policy": "union", "separator": " "}, "Invalid separator"),
        ({"policy": "union", "separator": "|\t"}, "Invalid separator"),
        ({"policy": "union", "separator": "$"}, "Invalid separator"),
        ({"policy": "union", "separator": "`"}, "Invalid separator"),
        ({"policy": "union", "separator": "\\"}, "Invalid separator"),
        ({"policy": "union", "separator": '"'}, "Invalid separator"),
        ({"policy": "union", "separator": 1}, "Invalid separator"),
        ({"policy": "union", "seperator": ";"}, "Invalid merge declaration"),
    ])
    def test_invalid_declarations_raise_naming_the_key(self, declaration, error):
        with pytest.raises(ValueError, match=error + r" for sl_options_merge\['all'\]\['fd'\]"):
            _merge(_union_event("a", "A", declaration))
