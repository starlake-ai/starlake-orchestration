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

from ai.starlake.common import is_valid_cron

from typing import List, Optional

class StarlakeTable():
    def __init__(self, name: str, final_name: Optional[str] = None, **kwargs):
        """Initializes a new StarlakeTable instance.

        Args:
            name (str): The required table name.
            final_name (str): The optional final table name.
        """
        self.name = name
        self.final_name = name if final_name is None else final_name

class StarlakeDomain():
    def __init__(self, name: str, final_name: str, tables: List[StarlakeTable], **kwargs):
        """Initializes a new StarlakeDomain instance.

        Args:
            name (str): The required domain name.
            final_name (str): The required final domain name.
            tables (List[StarlakeTable]): The required tables.
        """
        self.name = name
        self.tables = tables

class StarlakeSchedule():
    def __init__(self, name: Optional[str], cron: Optional[str], domains: List[StarlakeDomain], **kwargs):
        """Initializes a new StarlakeSchedule instance.

        Args:
            name (str): The optional schedule name.
            cron (str): The optional cron.
            domains (List[StarlakeDomain]): The required domains.
        """
        self.name = None if name is None or name.lower().strip() == 'none' else name
        if cron is not None:
            if cron.lower().strip() == "none":
                cron = None
            elif not is_valid_cron(cron):
                raise ValueError(f"Invalid cron expression: {cron} for schedule {name}")
        self.cron = cron
        self.domains = domains

class StarlakeSchedules():
    def __init__(self, schedules: List[StarlakeSchedule], **kwargs):
        """Initializes a new StarlakeSchedules instance.

        Args:
            schedules (List[StarlakeSchedule]): The required schedules.
        """
        self.schedules = schedules

    def __repr__(self) -> str:
        return f"StarlakeSchedules(schedules={self.schedules})"

    def __str__(self) -> str:
        return f"StarlakeSchedules(schedules={self.schedules})"

    def __iter__(self):
        return iter(self.schedules)

    def __getitem__(self, index):
        return self.schedules[index]

    def __len__(self):
        return len(self.schedules)
