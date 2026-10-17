"""The shared HTTP client. Every collector uses it so secrets are masked
in URLs, exceptions and logs at every level."""

import asyncio
import logging
import re
import time

import httpx

MASK = "***"
_KEY_PARAM = re.compile(r"(?i)((?:api[_-]?key|apikey|token|key)=)[^&\s\"']+")


class Redactor:
    def __init__(self, secrets: list[str] | None = None) -> None:
        self._secrets = [s for s in (secrets or []) if s]

    def add(self, secret: str | None) -> None:
        if secret:
            self._secrets.append(secret)

    def __call__(self, text: str) -> str:
        for s in self._secrets:
            text = text.replace(s, MASK)
        return _KEY_PARAM.sub(rf"\g<1>{MASK}", text)


redact = Redactor()


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = None
        return True


def install_log_redaction() -> None:
    for handler in logging.getLogger().handlers:
        handler.addFilter(RedactingFilter())


class ProviderError(Exception):
    """A provider call failed. The message is already sanitized and safe to
    show in /api/status."""

    def __init__(self, error_class: str, message: str) -> None:
        self.error_class = error_class
        super().__init__(redact(message))


def make_client(timeout: float = 30.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "fuelwatch (+https://github.com/chris-sievers/fuelwatch)"},
    )


async def get_json(client: httpx.AsyncClient, url: str, **params) -> object:
    """GET a JSON document, turning failures into sanitized ProviderErrors."""
    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise ProviderError(
            "http_status", f"{e.response.status_code} from {e.request.url}"
        ) from None
    except httpx.TransportError as e:
        raise ProviderError("transport", f"{type(e).__name__} calling {redact(url)}") from None
    except ValueError:
        raise ProviderError("bad_json", f"non-JSON response from {redact(url)}") from None


RETRY_STATUSES = (429, 502, 503, 504)  # rate limits and gateway hiccups


class PacedClient:
    """The shared client, paced per host and counting requests per host.

    Collectors only call ``client.get``, so this wraps that: requests to one
    host are spaced at least ``min_interval`` seconds apart (per-host values
    in ``per_host``), and a 429, 502, 503 or 504 is retried after its Retry-After (or
    a doubling delay), up to ``retries`` times, with that host's spacing
    doubled for the rest of the run. Anything else passes through.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        min_interval: float = 0.0,
        per_host: dict[str, float] | None = None,
        *,
        retries: int = 3,
        retry_delay: float = 30.0,
        max_retry_delay: float = 600.0,
        sleep=None,
        clock=None,
    ) -> None:
        self._client = client
        self._default = min_interval
        self._interval = dict(per_host or {})
        self._retries = retries
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._sleep = sleep or asyncio.sleep
        self._clock = clock or time.monotonic
        self._locks: dict[str, asyncio.Lock] = {}
        self._last: dict[str, float] = {}
        self.requests: dict[str, int] = {}
        self.rate_limited: dict[str, int] = {}

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def _pace(self, host: str) -> None:
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            interval = self._interval.get(host, self._default)
            last = self._last.get(host)
            if last is not None:
                wait = last + interval - self._clock()
                if wait > 0:
                    await self._sleep(wait)
            self._last[host] = self._clock()

    async def get(self, url, **kwargs) -> httpx.Response:
        host = httpx.URL(str(url)).host
        delay = self._retry_delay
        for attempt in range(self._retries + 1):
            await self._pace(host)
            self.requests[host] = self.requests.get(host, 0) + 1
            resp = await self._client.get(url, **kwargs)
            if resp.status_code not in RETRY_STATUSES or attempt == self._retries:
                return resp
            self.rate_limited[host] = self.rate_limited.get(host, 0) + 1
            self._interval[host] = max(2 * self._interval.get(host, self._default), 1.0)
            ra = _retry_after(resp)
            wait = delay if ra is None else ra
            if ra is None:
                delay = min(delay * 2, self._max_retry_delay)
            logging.getLogger(__name__).warning(
                "%s answered %s; retrying in %.0f s", host, resp.status_code, wait
            )
            await self._sleep(min(wait, self._max_retry_delay))
        return resp  # pragma: no cover - the loop always returns


def _retry_after(resp: httpx.Response) -> float | None:
    try:
        v = float(resp.headers.get("retry-after", ""))
    except ValueError:
        return None
    return v if v >= 0 else None
