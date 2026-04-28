from enum import Enum
from json import dumps
from typing import Literal, NotRequired, TypedDict, cast

from annotated_types import T

TraderaStatusStates = Literal[
    "BadRequest",
    "Unauthorized",
    "Forbidden",
    "TooManyRequests",
    "InternalServerError",
]


class TraderaStatusCode(Enum):
    BADREQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    TOOMANYREQUESTS = 429
    INTERNALSERVERERROR = 500

    def string(self: "TraderaStatusCode") -> TraderaStatusStates:
        match self:
            case TraderaStatusCode.BADREQUEST:
                return "BadRequest"
            case TraderaStatusCode.UNAUTHORIZED:
                return "Unauthorized"
            case TraderaStatusCode.FORBIDDEN:
                return "Forbidden"
            case TraderaStatusCode.TOOMANYREQUESTS:
                return "TooManyRequests"
            case TraderaStatusCode.INTERNALSERVERERROR:
                return "InternalServerError"
            case _:
                raise ValueError("Invalid status code")


class Taggable(TypedDict, total=False):
    __kind__: NotRequired[str]


class TraderaError(Taggable):
    code: TraderaStatusCode
    message: str


class TraderaErrorResponse(Taggable):
    error: TraderaError


class APIError(Exception):
    """
    Returned for local ShipStation responses such as during configuration.
    """

    __slots__ = ("status_code", "details")

    def __init__(self, status: int, detail: str):
        self.status_code = status
        self.details = detail

    def json(self) -> TraderaErrorResponse:
        return cast(
            TraderaErrorResponse,
            {
                "error": {
                    "code": self.status_code,
                    "message": self.details,
                }
            },
        )

    def __str__(self) -> str:
        outdict = {
            "error": {
                "code": self.status_code,
                "message": self.details,
            }
        }
        return dumps(outdict, indent=4, ensure_ascii=False)

    @property
    def text(self) -> str:
        return self.__str__()

    @property
    def content(self) -> bytes:
        return self.__str__().encode("utf-8")
