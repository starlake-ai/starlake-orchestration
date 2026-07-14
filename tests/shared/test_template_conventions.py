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

"""Cross-orchestrator template convention scan (AC1, AC4).

Runs on the ``orchestration`` CI leg — no orchestrator package imports
anywhere in this file; only ``jinja2``/``pytest`` from the test extra.
Any core-module change triggers this leg (and fans out to all legs), so
cross-module conventions re-run whenever the core changes or on
``workflow_dispatch``; single-module template edits are gated by that
module's own ``test_{orch}_templates.py``.
"""

from __future__ import annotations

import re

import pytest

from tests.shared.template_test_utils import (
    HEADER_OPTION_RE,
    MODULE_RESOURCES,
    ORCHESTRATOR_ENUMS,
    ORCHESTRATOR_SNIPPETS,
    TEMPLATE_NAME_RE,
    parse_header_options,
    user_facing_templates,
)

# {% include 'path' %} or {% include "path" %}
_INCLUDE_RE = re.compile(r"""{%\s*include\s+['"](?P<path>[^'"]+)['"]\s*%}""")


def _includes(template_path):
    return [m.group("path") for m in _INCLUDE_RE.finditer(
        template_path.read_text(encoding="utf-8")
    )]


class TestTemplateNamingConvention:

    def test_every_user_facing_template_matches_naming_convention(self):
        offenders = [
            p.name for p in user_facing_templates()
            if not TEMPLATE_NAME_RE.match(p.name)
        ]
        assert not offenders, (
            f"Templates violating {{orchestrator}}__scheduled_{{type}}__{{env}}.py.j2: "
            f"{offenders}"
        )


class TestTemplateHeaderConvention:

    def test_every_user_facing_template_has_documented_header(self):
        undocumented = []
        for path in user_facing_templates():
            options = parse_header_options(path)
            if not options:
                undocumented.append(path.name)
        assert not undocumented, (
            f"Templates missing a '# - option: description [OPTIONAL|REQUIRED]' "
            f"header block: {undocumented}"
        )

    def test_header_block_precedes_first_include(self):
        offenders = []
        for path in user_facing_templates():
            text = path.read_text(encoding="utf-8")
            first_include = text.find("{%")
            first_option = text.find("# - ")
            if first_option == -1 or (first_include != -1 and first_option > first_include):
                offenders.append(path.name)
        assert not offenders, f"Header block must precede includes in: {offenders}"

    def test_every_header_option_line_is_cli_parseable(self):
        """Every `# - ` line must parse exactly as the Starlake CLI parses it.

        DagTemplateOption.fromLine (AnyTemplateLoader.scala:39-62) silently
        DROPS any `# - ` line whose split(':') is not exactly 2 parts or that
        lacks an [OPTIONAL]/[REQUIRED] tag — an unparseable line is an option
        invisible to `starlake dag-templates` and to the Epic 4 AI skills.
        """
        offenders = []
        for path in user_facing_templates():
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.startswith("#"):
                    break  # header block ends at the first non-comment line
                if line.startswith("# - ") and not HEADER_OPTION_RE.match(line):
                    offenders.append((path.name, line))
        assert not offenders, (
            f"Header option lines the Starlake CLI cannot parse: {offenders}"
        )


class TestSnippetComposition:

    def test_every_template_includes_its_orchestrator_snippet(self):
        offenders = []
        for path in user_facing_templates():
            prefix = path.name.split("__", 1)[0]
            snippet = ORCHESTRATOR_SNIPPETS.get(prefix)
            if snippet is None:
                # Unknown prefix — the naming-convention test reports it too;
                # list it here instead of raising KeyError mid-scan.
                offenders.append((path.name, f"unknown orchestrator prefix '{prefix}'"))
                continue
            if snippet not in _includes(path):
                offenders.append((path.name, snippet))
        assert not offenders, f"Templates missing their orchestrator snippet: {offenders}"

    @pytest.mark.parametrize("prefix", sorted(ORCHESTRATOR_SNIPPETS))
    def test_snippet_includes_common_and_sets_enum(self, prefix):
        snippet_rel = ORCHESTRATOR_SNIPPETS[prefix]
        module = "orchestration" if prefix == "starlake" else prefix
        snippet_path = MODULE_RESOURCES[module] / snippet_rel
        assert snippet_path.is_file(), f"Missing snippet: {snippet_path}"
        text = snippet_path.read_text(encoding="utf-8")
        assert "templates/dags/__common__.py.j2" in text
        assert (
            f"orchestrator = StarlakeOrchestrator.{ORCHESTRATOR_ENUMS[prefix]}" in text
        )
