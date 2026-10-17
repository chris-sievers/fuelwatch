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


_FORMATTER = logging.Formatter()


class RedactingFilter(logging.Filter):
    """Masks secrets in the message, and in any traceback or stack: the
    exception is formatted here and redacted into ``exc_text`` (which
    formatters print as-is), and ``exc_info`` is cleared so no handler
    formats the raw exception again."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            text = record.getMessage()
        except Exception:  # noqa: BLE001 - a malformed log call must never raise here
            text = f"{record.msg} (bad log arguments)"
        record.msg = redact(str(text))
        record.args = None
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = _FORMATTER.formatException(record.exc_info)
            record.exc_info = None
        if record.exc_text:
            record.exc_text = redact(record.exc_text)
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
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
MAX_SPACING = 60.0  # the most a host's spacing grows to after rate limiting


class Stopped(ProviderError):
    def __init__(self) -> None:
        super().__init__("stopped", "shutting down")


class PacedClient:
    """The shared client, paced per host and counting requests per host.

    Collectors only call ``client.get``, so this wraps that.

    - Requests to one host start at least its spacing apart: ``per_host``
      values, else ``min_interval``. Each request reserves its start slot
      under a short lock and then sleeps outside it.
    - A 429, 502, 503 or 504 is retried up to ``retries`` times, after its
      numeric Retry-After (an HTTP-date Retry-After is ignored) or else a
      delay that starts at ``retry_delay`` and doubles; every wait is capped
      at ``max_retry_delay``.
    - A ``get`` that met such a response doubles that host's spacing once
      (at least 1 s, at most ``max_spacing``). The next successful response
      resets it to the configured value.
    - With ``stop`` set (an asyncio.Event), every wait ends at once when it
      is set, raising ``Stopped`` (a ProviderError).
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
        max_spacing: float = MAX_SPACING,
        stop: asyncio.Event | None = None,
        sleep=None,
        clock=None,
    ) -> None:
        self._client = client
        self._default = min_interval
        self._base = dict(per_host or {})
        self._interval: dict[str, float] = {}
        self._retries = retries
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._max_spacing = max_spacing
        self.stop = stop
        self._sleep = sleep or asyncio.sleep
        self._clock = clock or time.monotonic
        self._lock = asyncio.Lock()
        self._next: dict[str, float] = {}
        self.requests: dict[str, int] = {}
        self.rate_limited: dict[str, int] = {}

    def __getattr__(self, name):
        return getattr(self._client, name)

    def base_spacing(self, host: str) -> float:
        return self._base.get(host, self._default)

    def spacing(self, host: str) -> float:
        return self._interval.get(host, self.base_spacing(host))

    async def _wait(self, seconds: float) -> None:
        """Sleep, ending early with Stopped when `stop` is set."""
        stop = self.stop
        if stop is not None and stop.is_set():
            raise Stopped()
        if seconds <= 0:
            return
        if stop is None:
            await self._sleep(seconds)
            return
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        stopper = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for f in (sleeper, stopper):
                f.cancel()
            await asyncio.gather(sleeper, stopper, return_exceptions=True)
        if stop.is_set():
            raise Stopped()

    async def _pace(self, host: str) -> None:
        async with self._lock:  # held only to reserve a slot, never while sleeping
            now = self._clock()
            start = max(now, self._next.get(host, now))
            self._next[host] = start + self.spacing(host)
        await self._wait(start - now)

    async def get(self, url, **kwargs) -> httpx.Response:
        host = httpx.URL(str(url)).host
        delay = self._retry_delay
        slowed = False
        for attempt in range(self._retries + 1):
            await self._pace(host)
            self.requests[host] = self.requests.get(host, 0) + 1
            resp = await self._client.get(url, **kwargs)
            if resp.status_code not in RETRY_STATUSES:
                self._interval.pop(host, None)  # healthy again: back to the base spacing
                return resp
            self.rate_limited[host] = self.rate_limited.get(host, 0) + 1
            if not slowed:
                slowed = True
                grown = max(2 * self.spacing(host), 1.0)
                self._interval[host] = min(grown, max(self._max_spacing, self.base_spacing(host)))
            if attempt == self._retries:
                return resp
            ra = _retry_after(resp)
            if ra is None:
                wait, delay = delay, delay * 2
            else:
                wait = ra
            wait = min(wait, self._max_retry_delay)
            logging.getLogger(__name__).warning(
                "%s answered %s; retrying in %.0f s", host, resp.status_code, wait
            )
            await self._wait(wait)
        return resp  # pragma: no cover - the loop always returns


def _retry_after(resp: httpx.Response) -> float | None:
    try:
        v = float(resp.headers.get("retry-after", ""))
    except ValueError:
        return None
    return v if v >= 0 else None  # NaN fails this too
