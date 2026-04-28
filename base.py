from asyncio import Lock
from dataclasses import dataclass
from hashlib import sha256
from json import JSONDecodeError, dumps
from logging import getLogger
from typing import Final, Literal, Self, TypeVar, cast
from uuid import uuid4

from httpx import AsyncClient, Limits, Response
from httpx._types import HeaderTypes
from pydantic import EmailStr, HttpUrl, Secret, SecretStr

from ._types import APIError, TraderaErrorResponse

T = TypeVar("T")
SecretInt = Secret[int]
LOGGER = getLogger("asyncTradera")
VERSION: Final[str] = "0.0.0.1"
ENDPOINT: Final[str] = "https://api.tradera.com/v4/"
HEADERS: Final[dict[str, str | int | None]] = {
    "X-App-Id": None,
    "X-App-Key": None,
}


@dataclass(slots=True, frozen=True)
class ConnectionConfig:
    timeout: int = 500
    max_connections: int = 20
    max_keepalive_connections: int = 10
    http2: bool = False
    retries: int = 4
    user_agent: str = f"asyncTradera/{VERSION}"

    def __hash__(self: "ConnectionConfig") -> int:
        raw = f"{self.timeout}:{self.max_connections}:{self.max_keepalive_connections}:{self.user_agent}:{self.retries}:{self.http2}"
        digest = int.from_bytes(
            sha256(raw.encode("utf-8")).digest()[:8], "big", signed=True
        )
        return -2 if digest == -1 else digest

    def __eq__(self: "ConnectionConfig", other: object) -> bool:
        if not isinstance(other, ConnectionConfig):
            return NotImplemented
        return (
            self.timeout == other.timeout
            and self.max_connections == other.max_connections
            and self.max_keepalive_connections == other.max_keepalive_connections
            and self.user_agent == other.user_agent
            and self.retries == other.retries
            and self.http2 == other.http2
        )


class TraderaConnection:
    __slots__: tuple[str, ...] = (
        "_client",
        "_ref",
        "_client",
        "_config",
        "_headers",
        "_app_id",
        "_app_key",
        "_lock",
        "_uid",
    )

    def __init__(
        self: "TraderaConnection", app_id: int, app_key: str, config: ConnectionConfig
    ) -> None:
        self._app_id: SecretInt = SecretInt(app_id)
        self._app_key: SecretStr = SecretStr(app_key)
        self._config: ConnectionConfig = config
        self._headers = HEADERS.copy()
        self._headers["X-App-Id"] = self._app_id.get_secret_value()
        self._headers["X-App-Key"] = self._app_key.get_secret_value()
        self._headers["User-Agent"] = config.user_agent
        self._uid = self.hash(app_id, app_key, config)
        self._lock: Lock = Lock()
        self._ref: int = 0
        self._client: AsyncClient | None = None

        if not app_id:
            raise ValueError("app_id must be a non-zero integer")
        if not app_key:
            raise ValueError("app_key must be a non-empty string")

    async def start(self: "TraderaConnection") -> None:
        async with self._lock:
            if self._client is None or self._client.is_closed:
                self._client = AsyncClient(
                    base_url=ENDPOINT,
                    headers=cast(HeaderTypes, self._headers),
                    timeout=self._config.timeout,
                    http2=self._config.http2,
                    limits=Limits(
                        max_connections=self._config.max_connections,
                        max_keepalive_connections=self._config.max_keepalive_connections,
                    ),
                )

            self._ref += 1

    async def close(self: "TraderaConnection", force: bool = False) -> None:
        async with self._lock:
            self._ref = max(0, self._ref - 1)
            if self._ref <= 0 or force:
                self._ref = 0
                if self._client is not None:
                    await self._client.aclose()
                self._client = None

    async def request(
        self: "TraderaConnection",
        method: Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        url: str,
        **kwargs: dict[str, str | int | bool | EmailStr | HttpUrl | None],
    ) -> Response:

        self_start = False

        if self._client is None:
            await self.start()
            self_start = True

        response = await self._client.request(method, url, **kwargs)  # type: ignore[arg-type]

        if self_start:
            await self.close()
        return response

    @property
    def config(self: "TraderaConnection") -> ConnectionConfig:
        return self._config

    @property
    def uid(self: "TraderaConnection") -> int:
        return self._uid

    @property
    def is_active(self: "TraderaConnection") -> bool:
        return self._client is not None and not self._client.is_closed

    @property
    def ref_count(self: "TraderaConnection") -> int:
        return self._ref

    @property
    def app_id(self: "TraderaConnection") -> int:
        return self._app_id.get_secret_value()

    @property
    def app_key(self: "TraderaConnection") -> str:
        return self._app_key.get_secret_value()

    def __eq__(self: "TraderaConnection", other: object) -> bool:
        if not isinstance(other, TraderaConnection):
            return NotImplemented

        otherc = cast("TraderaConnection", other)
        return (
            self._app_key == otherc._app_key
            and self._app_id == otherc._app_id
            and self._config == otherc._config
        )

    @staticmethod
    def hash(
        app_id: int,
        app_key: str,
        config: ConnectionConfig,
    ) -> int:
        raw = f"{app_id}:{app_key or ''}:{hash(config)}"
        digest = int.from_bytes(
            sha256(raw.encode("utf-8")).digest()[:8], "big", signed=True
        )
        return -2 if digest == -1 else digest

    def __hash__(self) -> int:
        return self._uid


class TraderaClient:
    __slots__: tuple[str, ...] = ("_pool", "_pool_lock")

    def __init__(self: "TraderaClient") -> None:
        self._pool: dict[int, TraderaConnection] = {}
        self._pool_lock: Lock = Lock()

    @staticmethod
    def _apply_identity_tag(
        payload: object,
        return_type: type[T],
    ) -> object:
        if isinstance(payload, dict):
            tagged = dict(payload)
            tagged["__kind__"] = return_type.__name__
            return tagged

        return payload

    @staticmethod
    def parse_unknown_exception(
        exception: Exception | APIError,
    ) -> tuple[Literal[500], TraderaErrorResponse]:
        """
        Parses an unknown exception and returns a standardized error response.
        Args:
            exception (Exception): The exception to parse.
        Returns:
            tuple[Literal[500], ErrorResponse]: A tuple containing the status code and the error details.
        """
        out = (
            cast(APIError, exception).json()
            if isinstance(exception, APIError)
            else cast(
                TraderaErrorResponse,
                {
                    "error": {
                        "code": 500,
                        "message": str(exception),
                    }
                },
            )
        )

        return 500, out

    def validate_response(
        self: "TraderaClient",
        res: Response | APIError,
        accepted_statuses: tuple[int, ...],
        return_type: type[T],
        identity: bool = False,
    ) -> tuple[int, TraderaErrorResponse | T]:
        try:
            payload = cast(object, res.json())
        except JSONDecodeError as e:
            return self.parse_unknown_exception(
                Exception(f"JSON decode error: {e}. Raw response: {res.text[:500]}")
            )

        if res.status_code not in accepted_statuses:
            if isinstance(payload, dict) and "errors" in payload:
                return res.status_code, cast(TraderaErrorResponse, payload)

            payload = payload if isinstance(payload, str) else dumps(payload)

            raise APIError(res.status_code, payload)

        if identity:
            payload = self._apply_identity_tag(payload, return_type)

        return res.status_code, cast(T, payload)

    async def _add_connection(
        self: "TraderaClient", connection: TraderaConnection
    ) -> int:
        async with self._pool_lock:
            if connection.uid not in self._pool:
                self._pool[connection.uid] = connection

            LOGGER.info(
                f"_add_connection: Added connection with UID {connection.uid} to pool."
            )

            return connection.uid

    async def _evict_connection(
        self: "TraderaClient", uid: int, force: bool = False
    ) -> None:
        async with self._pool_lock:
            connection = self._pool.get(uid, None)
            if not connection:
                LOGGER.warning(
                    f"_evict_connection: No connection found with UID {uid} for eviction."
                )
                return

            del self._pool[uid]
            LOGGER.info(
                f"_evict_connection: Evicted connection with UID {uid} from pool."
            )

    async def _get_connection(
        self: "TraderaClient",
        uid: int | None = None,
        app_id: int | None = None,
        app_key: str | None = None,
        config: ConnectionConfig | None = None,
    ) -> TraderaConnection | None:
        async with self._pool_lock:
            if uid is not None:
                connection = self._pool.get(uid, None)
                if connection is None:
                    LOGGER.warning(
                        f"_get_connection: No connection found with UID {uid}."
                    )
                    return None
                return connection

            if config:
                connection = TraderaConnection.hash(app_id or 0, app_key or "", config)
                return self._pool.get(connection, None)

            return None

    async def connect(
        self: "TraderaClient",
        uid: int | None = None,
        app_id: int | None = None,
        app_key: str | None = None,
        config: ConnectionConfig | None = None,
    ) -> TraderaConnection:
        connection: TraderaConnection | None = await self._get_connection(
            uid, app_id, app_key, config
        )

        if connection is None:
            connection = TraderaConnection(
                app_id=app_id or 0,
                app_key=app_key or "",
                config=config or ConnectionConfig(),
            )
            uid = await self._add_connection(connection)
            LOGGER.info(f"connect: Created new connection with UID {uid}.")

        return connection

    async def start(
        self: "TraderaClient",
        uid: int | None = None,
        app_id: int | None = None,
        app_key: str | None = None,
        config: ConnectionConfig | None = None,
        connection: TraderaConnection | None = None,
    ) -> TraderaConnection:

        if connection is not None:
            await connection.start()
            return connection

        connection = await self._get_connection(uid, app_id, app_key, config)
        if connection is None:
            connection = await self.connect(uid, app_id, app_key, config)
        if connection is None:
            raise Exception("Failed to establish connection")

        await connection.start()
        return connection

    async def close(
        self: "TraderaClient",
        uid: int | None = None,
        app_id: int | None = None,
        app_key: str | None = None,
        config: ConnectionConfig | None = None,
        connection: TraderaConnection | None = None,
        force: bool = False,
    ) -> None:

        conn = (
            connection
            if connection is not None
            else await self._get_connection(uid, app_id, app_key, config)
        )

        if conn is None:
            LOGGER.warning("close: No connection found to close.")
            raise Exception("No connection found to close")

        await conn.close(force=force)
        if conn.ref_count == 0:
            await self._evict_connection(conn.uid, force=force)
