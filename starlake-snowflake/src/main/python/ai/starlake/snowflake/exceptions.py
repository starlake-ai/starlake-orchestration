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

class StarlakeSnowflakeError(Exception):
    """The base exception class for all Starlake Python related Errors."""

    def __init__(self, message: str, error_code: int = 1) -> None:
        self.__message = message
        self.__error_code = error_code
        super().__init__(self.message)

    @property
    def message(self):
        return self.__message

    @property
    def error_code(self):
        return self.__error_code

    def __str__(self):
        return f"[{self.message} (Error Code: {self.error_code})]"
