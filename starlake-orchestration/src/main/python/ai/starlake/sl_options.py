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

"""Merge of the run variables (``sl_options``) carried by several dataset
events (issue #158).

One triggered run may receive several events: Airflow coalesces the asset
events emitted since the previous asset-triggered run, and a Dagster sensor
tick reads the materializations of several upstream assets. Each event may
carry ``sl_options`` sections (``{"all": {key: value}, "<domain.task>":
{key: value}}``). By default the merge is fail-loud: the same key carried with
two different values raises, since conflicting run variables must stop the
run, not silently pick one.

A producer may declare, in ``sl_options_merge`` (same sections), that a key
holds a set of values::

    {"sl_options": {"all": {"file_dates": "2026-11-01|2026-11-02"}},
     "sl_options_merge": {"all": {"file_dates": {"policy": "union", "separator": "|"}}}}

- ``union``: the merged value is the sorted, de-duplicated union of the
  elements of every event, split on ``separator`` (default ``|``), trimmed,
  empty elements dropped (a list value gives its items as elements). The
  declaration describes the value's type, so it also applies to the events
  that carry the key without declaring it, and a single event is normalized
  the same way.
- ``strict`` (the default for an undeclared key): equal values merge,
  different values raise.

Two events declaring different policies or separators for the same key raise.

This module is import-light (no orchestrator import): starlake-airflow and
starlake-dagster share it and turn its ``ValueError`` into their own error.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Tuple

from ai.starlake.common import StarlakeParameters

STRICT_POLICY = "strict"
UNION_POLICY = "union"
DEFAULT_UNION_SEPARATOR = "|"

_OPTIONS = StarlakeParameters.OPTIONS_PARAMETER.value
_OPTIONS_MERGE = StarlakeParameters.OPTIONS_MERGE_PARAMETER.value


def _entries(payload: Any) -> Iterator[Tuple[str, str, Any]]:
    """Yield (section, key, value) of a {section: {key: value}} payload; a
    payload or a section that is not a dict is ignored."""
    if not isinstance(payload, dict):
        return
    for section, entries in payload.items():
        if isinstance(entries, dict):
            for key, value in entries.items():
                yield section, key, value


def _declaration_entries(payload: Any, source: Any) -> Iterator[Tuple[str, str, Any]]:
    """Yield (section, key, declaration) of a sl_options_merge payload, which
    must be a {section: {key: declaration}} dict when present: a malformed one
    must not silently leave its keys strict."""
    if payload is None:
        return
    if not isinstance(payload, dict):
        raise ValueError(
            f"Invalid {_OPTIONS_MERGE} carried by {source}: expected a dict of sections "
            f"{{section: {{key: declaration}}}}, got {payload!r}."
        )
    for section, entries in payload.items():
        if not isinstance(entries, dict):
            raise ValueError(
                f"Invalid {_OPTIONS_MERGE}['{section}'] carried by {source}: expected a dict "
                f"{{key: declaration}}, got {entries!r}."
            )
        for key, declaration in entries.items():
            yield section, key, declaration


_DECLARATION_FIELDS = ("policy", "separator")

# Characters a separator must not contain: the comma splits the --options pairs,
# whitespace splits the Cloud Run arguments, and the others are still active
# within the double quotes that wrap --options in the shell transports.
_FORBIDDEN_SEPARATOR_CHARACTERS = ",$`\\\""


def _normalize_declaration(section: str, key: str, declaration: Any) -> Dict[str, str]:
    """Validate a merge declaration and return its normalized form."""
    name = f"{_OPTIONS_MERGE}['{section}']['{key}']"
    if not isinstance(declaration, dict):
        raise ValueError(
            f"Invalid merge declaration for {name}: expected a dict such as "
            f"{{'policy': '{UNION_POLICY}', 'separator': '{DEFAULT_UNION_SEPARATOR}'}}, got {declaration!r}."
        )
    # a misspelt field (e.g. 'seperator') would silently fall back to its default
    unknown = sorted(str(field) for field in declaration if field not in _DECLARATION_FIELDS)
    if unknown:
        raise ValueError(
            f"Invalid merge declaration for {name}: unknown field(s) {', '.join(unknown)} "
            f"(expected {', '.join(_DECLARATION_FIELDS)})."
        )
    policy = declaration.get("policy", None)
    if policy == STRICT_POLICY:
        return {"policy": STRICT_POLICY}
    if policy == UNION_POLICY:
        separator = declaration.get("separator", None)
        if separator is None:
            separator = DEFAULT_UNION_SEPARATOR
        if (
            not isinstance(separator, str)
            or not separator
            or any(c.isspace() or c in _FORBIDDEN_SEPARATOR_CHARACTERS for c in separator)
        ):
            raise ValueError(
                f"Invalid separator for {name}: expected a non-empty string without whitespace nor any of "
                f"{' '.join(_FORBIDDEN_SEPARATOR_CHARACTERS)} (they break the --options transport), got {separator!r}."
            )
        return {"policy": UNION_POLICY, "separator": separator}
    raise ValueError(
        f"Invalid merge policy for {name}: expected '{UNION_POLICY}' or '{STRICT_POLICY}', got {policy!r}."
    )


def _union(values: List[Any], separator: str) -> str:
    elements = set()
    for value in values:
        if value is None:
            continue
        parts = [str(item) for item in value] if isinstance(value, (list, tuple)) else str(value).split(separator)
        for element in parts:
            element = element.strip()
            if element:
                elements.add(element)
    return separator.join(sorted(elements))


def merge_sl_options(
    carriers: Iterable[Tuple[Any, Any, Any]],
    origin: str,
    recovery: str,
    superseded: Iterable[Tuple[Any, Any, Any]] = (),
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Dict[str, str]]]]:
    """Merge the sl_options sections carried by several events.

    Args:
        carriers: one ``(source, sections, declarations)`` per event:
            ``source`` names the event in error messages (a dataset URI, an
            asset key), ``sections`` is its ``sl_options`` payload and
            ``declarations`` its ``sl_options_merge`` payload.
        origin: what the events are, in error messages (e.g. "triggering
            dataset events").
        recovery: how to recover from conflicting values, appended to their
            error message.
        superseded: events replaced by a later event of the same source (the
            earlier materializations of a Dagster asset, whose sensor reads
            the latest one), same shape: their declarations count, but their
            values only for the keys declared union.

    Returns:
        ``(sections, declarations)``: the merged ``{section: {key: value}}``,
        and the normalized declarations ``{section: {key: {"policy": ...}}}``
        of the merged keys that are declared (for a run that relays them
        downstream).

    Raises:
        ValueError: on an invalid declaration, on two events declaring a key
            differently, or on different values of a key not declared union.
    """
    carriers = list(carriers or [])
    superseded = list(superseded or [])

    declarations: Dict[Tuple[str, str], Tuple[Any, Dict[str, str]]] = {}
    for source, _, event_declarations in carriers + superseded:
        for section, key, declaration in _declaration_entries(event_declarations, source):
            normalized = _normalize_declaration(section, key, declaration)
            declared = declarations.get((section, key), None)
            if declared is None:
                declarations[(section, key)] = (source, normalized)
            elif declared[1] != normalized:
                raise ValueError(
                    f"Conflicting merge declarations for {_OPTIONS_MERGE}['{section}']['{key}'] across {origin} "
                    f"({declared[0]}, {source}): {declared[1]} != {normalized}. Every event that declares "
                    f"how a key merges must declare the same policy and separator."
                )

    def is_union(section: str, key: str) -> bool:
        declared = declarations.get((section, key), None)
        return declared is not None and declared[1]["policy"] == UNION_POLICY

    carried: Dict[Tuple[str, str], List[Tuple[Any, Any]]] = {}
    for source, event_sections, _ in carriers:
        for section, key, value in _entries(event_sections):
            carried.setdefault((section, key), []).append((source, value))
    for source, event_sections, _ in superseded:
        for section, key, value in _entries(event_sections):
            if is_union(section, key):
                carried.setdefault((section, key), []).append((source, value))

    sections: Dict[str, Dict[str, Any]] = {}
    for (section, key), values in carried.items():
        declaration = declarations.get((section, key), (None, {"policy": STRICT_POLICY}))[1]
        if declaration["policy"] == UNION_POLICY:
            merged = _union([value for _, value in values], declaration["separator"])
        else:
            first_source, merged = values[0]
            for source, value in values[1:]:
                if str(merged) != str(value):
                    raise ValueError(
                        f"Conflicting values for {_OPTIONS}['{section}']['{key}'] across {origin} "
                        f"({first_source}, {source}): '{merged}' != '{value}'. {recovery} If the key holds "
                        f"a set of values, its producer can declare it in {_OPTIONS_MERGE} "
                        f"(policy '{UNION_POLICY}') to merge them by union."
                    )
                merged = value
        sections.setdefault(section, {})[key] = merged

    merge: Dict[str, Dict[str, Dict[str, str]]] = {}
    for (section, key), (_, declaration) in declarations.items():
        if (section, key) in carried:
            merge.setdefault(section, {})[key] = declaration
    return sections, merge
