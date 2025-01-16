import msgspec

from nautilus_trader.adapters.dydx.endpoints.endpoint import KMSHttpEndpoint
from nautilus_trader.adapters.dydx.http.client import DYDXHttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class KMSTakeProfitOrderPostParams(msgspec.Struct, omit_defaults=True):

    size: float
    clientId: int
    subaccountNumber: int
    marketId: str
    orderSide: str
    price: float
    triggerPrice: float
    goodTilTimeInSeconds: int
    chainId: str = "dydx-testnet-v4"


class KMSTakeProfitOrderResponse(msgspec.Struct, forbid_unknown_fields=False):

    code: int
    height: int
    txIndex: int
    transactionHash: str
    gasUsed: str
    gasWanted: str


class KMSTakeProfitOrderEndpoint(KMSHttpEndpoint):

    def __init__(self, client: DYDXHttpClient) -> None:

        url_path = "/createTakeProfitOrder"
        super().__init__(
            client=client,
            url_path=url_path,
            name="KMSTakeProfitOrderEndpoint",
        )
        self.method_type = HttpMethod.POST
        self._decoder = msgspec.json.Decoder(KMSTakeProfitOrderResponse)

    async def post(self, params: KMSTakeProfitOrderPostParams) -> KMSTakeProfitOrderResponse | None:

        raw = await self._method(self.method_type, params=params, url_path=self.url_path)

        if raw is not None:
            return self._decoder.decode(raw)

        return None
