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
"""
Define the candles / bars endpoint.
"""

# ruff: noqa: N815

import msgspec

from nautilus_trader.adapters.dydx.endpoints.endpoint import KMSHttpEndpoint
from nautilus_trader.adapters.dydx.http.client import DYDXHttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class KMSMarketOrderPostParams(msgspec.Struct, omit_defaults=True):
    size: float
    clientId: int
    subaccountNumber: int
    marketId: str
    orderSide: str
    price: float
    chainId: str = "dydx-testnet-v4"


class KMSMarketOrderResponse(msgspec.Struct, forbid_unknown_fields=False):
    transactionHash: str


class KMSMarketOrderEndpoint(KMSHttpEndpoint):
    def __init__(self, client: DYDXHttpClient) -> None:
        url_path = "/createMarketOrder"
        super().__init__(
            client=client,
            url_path=url_path,
            name="KMSMarketOrderEndpoint",
        )
        self.method_type = HttpMethod.POST
        self._decoder = msgspec.json.Decoder(KMSMarketOrderResponse)

    async def post(self, params: KMSMarketOrderPostParams) -> KMSMarketOrderResponse | None:
        raw = await self._method(self.method_type, params=params, url_path=self.url_path)

        if raw is not None:
            return self._decoder.decode(raw)

        return None
