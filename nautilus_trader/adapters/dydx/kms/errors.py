from typing import Any


class KMSError(Exception):
    """
    Define the class for all dYdX specific errors.
    """

    def __init__(self, status: int, message: str, headers: dict[str, Any]) -> None:
        """
        Define the base class for all dYdX specific errors.
        """
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers


def should_retry(error: BaseException) -> bool:
    """
    Determine if a retry should be attempted.

    Parameters
    ----------
    error : BaseException
        The error to check.

    Returns
    -------
    bool
        True if should retry, otherwise False.

    """
    if isinstance(error, KMSError):
        return True
        # return error.code in DYDX_RETRY_ERRORS_GRPC

    return False
