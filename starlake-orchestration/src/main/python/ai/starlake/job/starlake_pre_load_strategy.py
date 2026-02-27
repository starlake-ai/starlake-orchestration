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

from enum import Enum

# Aliases mapping alternative strategy names to canonical enum values
_STRATEGY_ALIASES: dict[str, str] = {
    "incoming": "imported",
}

class StarlakePreLoadStrategy(str, Enum):
    """Class with different pre load strategies."""

    IMPORTED = "imported"
    ACK = "ack"
    PENDING = "pending"
    NONE = "none"

    @classmethod
    def is_valid(cls, strategy: str) -> bool:
        """Validate a pre load strategy."""
        return strategy in cls.all_strategies() or strategy in _STRATEGY_ALIASES

    @classmethod
    def resolve(cls, strategy: str) -> 'StarlakePreLoadStrategy':
        """Resolve a strategy string, supporting aliases."""
        resolved = _STRATEGY_ALIASES.get(strategy, strategy)
        return cls(resolved)

    @classmethod
    def all_strategies(cls) -> set[str]:
        """Return all load strategies."""
        return set(cls.__members__.values())

    def __str__(self) -> str:
        return self.value
