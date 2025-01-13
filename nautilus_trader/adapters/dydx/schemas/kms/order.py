import msgspec


class KMSOrder(msgspec.Struct):
    size: float
    clientId: int
    subaccountNumber: int
    marketId: str
    orderSide: str
    price: float
    triggerPrice: float
    goodTilTimeInSeconds: int
    chainId: str
    order_type: str
