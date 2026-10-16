import httpx
import pytest
import respx

from fuelwatch.http import ProviderError, Redactor, get_json, make_client, redact


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
