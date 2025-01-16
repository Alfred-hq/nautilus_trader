from nautilus_trader.adapters.dydx.endpoints.kms.market_order import (
    KMSMarketOrderEndpoint,
    KMSMarketOrderPostParams,
    KMSMarketOrderResponse,
)
from nautilus_trader.adapters.dydx.endpoints.kms.stop_limit_order import (
    KMSStopLimitOrderEndpoint,
    KMSStopLimitOrderPostParams,
    KMSStopLimitOrderResponse,
)
from nautilus_trader.adapters.dydx.endpoints.kms.take_profit_order import (
    KMSTakeProfitOrderEndpoint,
    KMSTakeProfitOrderPostParams,
    KMSTakeProfitOrderResponse,
)
from nautilus_trader.adapters.dydx.kms.client import KMSHttpClient

# fmt: on
from nautilus_trader.common.component import LiveClock
from nautilus_trader.core.correctness import PyCondition

from nautilus_trader.adapters.dydx.endpoints.kms.cancel_order import (
    KMSCancelOrderEndpoint,
    KMSCancelOrderPostParams,
    KMSCancelOrderResponse,
)


class KMSTradeHttpAPI:
    """
    Define the account HTTP API endpoints.
    """

    def __init__(
        self,
        client: KMSHttpClient,
        clock: LiveClock,
    ) -> None:
        """
        Define the account HTTP API endpoints.
        """
        PyCondition.not_none(client, "client")
        self.client = client
        self._clock = clock

        self._endpoint_post_market_order = KMSMarketOrderEndpoint(client)
        self._endpoint_post_stop_limit_order = KMSStopLimitOrderEndpoint(client)
        self._endpoint_post_take_profit_order = KMSTakeProfitOrderEndpoint(client)
        self._endpoint_post_cancel_order = KMSCancelOrderEndpoint(client)
        # self._endpoint_get_perpetual_positions = DYDXGetPerpetualPositionsEndpoint(client)

    async def post_market_order(
        self,
        size: float,
        clientId: int,
        subaccountNumber: int,
        marketId: str,
        orderSide: str,
        price: float,
        chainId: str,
    ) -> KMSMarketOrderResponse | None:
        """
        Fetch the address subaccounts.
        """
        return await self._endpoint_post_market_order.post(
            KMSMarketOrderPostParams(
                size=size,
                clientId=clientId,
                subaccountNumber=subaccountNumber,
                marketId=marketId,
                orderSide=orderSide,
                price=price,
                chainId=chainId,
            )
        )

    async def post_stop_limit_order(
        self,
        size: float,
        clientId: int,
        subaccountNumber: int,
        marketId: str,
        orderSide: str,
        price: float,
        triggerPrice: float,
        goodTilTimeInSeconds: int,
        chainId: str,
    ) -> KMSStopLimitOrderResponse | None:
        """
        Fetch the subaccount.
        """
        return await self._endpoint_post_stop_limit_order.post(
            KMSStopLimitOrderPostParams(
                size=size,
                clientId=clientId,
                subaccountNumber=subaccountNumber,
                marketId=marketId,
                orderSide=orderSide,
                price=price,
                triggerPrice=triggerPrice,
                goodTilTimeInSeconds=goodTilTimeInSeconds,
                chainId=chainId,
            ),
        )

    async def post_take_profit_order(
        self,
        size: float,
        clientId: int,
        subaccountNumber: int,
        marketId: str,
        orderSide: str,
        price: float,
        triggerPrice: float,
        goodTilTimeInSeconds: int,
        chainId: str,
    ) -> KMSTakeProfitOrderResponse | None:
        """
        Fetch the subaccount.
        """
        return await self._endpoint_post_take_profit_order.post(
            KMSTakeProfitOrderPostParams(
                size=size,
                clientId=clientId,
                subaccountNumber=subaccountNumber,
                marketId=marketId,
                orderSide=orderSide,
                price=price,
                triggerPrice=triggerPrice,
                goodTilTimeInSeconds=goodTilTimeInSeconds,
                chainId=chainId,
            ),
        )

    async def post_cancel_order(
        self,
        clientId: int,
        subaccountNumber: int,
        marketId: str,
        chainId: str,
        good_til_date_secs: int,
    ) -> KMSCancelOrderResponse | None:
        """
        Fetch the subaccount.
        """
        return await self._endpoint_post_cancel_order.post(
            KMSCancelOrderPostParams(
                clientId=clientId,
                subaccountNumber=subaccountNumber,
                marketId=marketId,
                chainId=chainId,
                goodTilTimeInSeconds=good_til_date_secs,
            ),
        )
