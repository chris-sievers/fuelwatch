import logging

import httpx
import pytest
import respx

from fuelwatch.http import (
    ProviderError,
    RedactingFilter,
    Redactor,
    get_json,
    make_client,
    redact,
)


def test_redacts_known_secret_and_key_params():
    r = Redactor(["s3cr3t-value"])
    assert "s3cr3t-value" not in r("token s3cr3t-value here")
    assert r("https://x/y?api_key=abc123&a=1") == "https://x/y?api_key=***&a=1"


@respx.mock
async def test_http_error_message_is_sanitized():
    redact.add("planted-fake-key")
    respx.get("https://p.example/v2").mock(return_value=httpx.Response(500))
    async with make_client() as c:
        with pytest.raises(ProviderError) as ei:
            await get_json(c, "https://p.example/v2", api_key="planted-fake-key")
    assert "planted-fake-key" not in str(ei.value)
    assert ei.value.error_class == "http_status"


def _capture(level=logging.DEBUG):
    import io

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    handler.addFilter(RedactingFilter())
    logger = logging.getLogger("fuelwatch.test_redaction")
    logger.handlers = [handler]
    logger.setLevel(level)
    logger.propagate = False
    return logger, buf


def test_traceback_is_redacted():
    redact.add("PLANTED-traceback-key-9f3")
    logger, buf = _capture()

    def inner():
        secret = "PLANTED-traceback-key-9f3"  # noqa: F841 - in the frame, not the text
        raise RuntimeError("failed calling https://x/?api_key=PLANTED-traceback-key-9f3")

    try:
        inner()
    except RuntimeError:
        logger.debug("internal error", exc_info=True)
    logger.debug("stack", stack_info=True, extra={})
    out = buf.getvalue()
    assert "RuntimeError" in out and "Traceback" in out
    assert "PLANTED-traceback-key-9f3" not in out
    assert "api_key=***" in out


def test_exception_instance_in_exc_info_is_redacted():
    redact.add("PLANTED-instance-key-77a")
    logger, buf = _capture()
    err = ValueError("token PLANTED-instance-key-77a")
    logger.debug("boom", exc_info=err)
    assert "PLANTED-instance-key-77a" not in buf.getvalue()
    assert "ValueError" in buf.getvalue()
