# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2024 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import msgspec


class BybitUpdateSubApiResult(msgspec.Struct):
    id: str
    note: str
    apiKey: str
    readOnly: int
    secret: str
    permissions: dict[str, list[str]]
    ips: list[str]


class BybitUpdateSubApiResponse(msgspec.Struct):
    retCode: int
    retMsg: str
    result: BybitUpdateSubApiResult
