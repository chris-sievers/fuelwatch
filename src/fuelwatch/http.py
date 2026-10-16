"""The shared HTTP client. Every collector uses it so secrets are masked
in URLs, exceptions and logs at every level."""

import logging
import re

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
