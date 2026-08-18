from unittest.mock import MagicMock, patch

import pytest

import fetch_fx


def _response(status_code, json_body=None, headers=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = text
    if json_body is not None:
        resp.json.return_value = json_body
    return resp


SUCCESS_BODY = {
    "result": "success",
    "conversion_rates": {"KES": 129.5, "UGX": 3700.0, "TZS": 2600.0, "NGN": 1550.0, "USD": 1.0},
}


@patch.dict("os.environ", {}, clear=True)
def test_missing_api_key_raises():
    with pytest.raises(fetch_fx.FxFetchError, match="EXCHANGERATE_API_KEY"):
        fetch_fx.fetch_rates_from_api()


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.requests.get")
def test_success_first_try(mock_get):
    mock_get.return_value = _response(200, SUCCESS_BODY)
    rates = fetch_fx.fetch_rates_from_api()
    assert rates == {"KES": 129.5, "UGX": 3700.0, "TZS": 2600.0, "NGN": 1550.0}
    assert mock_get.call_count == 1
    # the key must be in the URL, not logged/printed elsewhere in this test
    assert "test-key" in mock_get.call_args[0][0]


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.requests.get")
def test_missing_currency_in_response_raises(mock_get):
    incomplete_body = {"result": "success", "conversion_rates": {"KES": 129.5}}
    mock_get.return_value = _response(200, incomplete_body)
    with pytest.raises(fetch_fx.FxFetchError, match="missing currencies"):
        fetch_fx.fetch_rates_from_api()


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.time.sleep")
@patch("fetch_fx.requests.get")
def test_retries_on_429_then_succeeds(mock_get, mock_sleep):
    mock_get.side_effect = [
        _response(429, headers={"Retry-After": "2"}),
        _response(200, SUCCESS_BODY),
    ]
    rates = fetch_fx.fetch_rates_from_api()
    assert rates["KES"] == 129.5
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(2.0)  # honours Retry-After exactly


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.time.sleep")
@patch("fetch_fx.requests.get")
def test_retries_on_500_then_succeeds(mock_get, mock_sleep):
    mock_get.side_effect = [
        _response(503, text="service unavailable"),
        _response(200, SUCCESS_BODY),
    ]
    rates = fetch_fx.fetch_rates_from_api()
    assert rates["NGN"] == 1550.0
    assert mock_get.call_count == 2


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.time.sleep")
@patch("fetch_fx.requests.get")
def test_retries_on_connection_error_then_succeeds(mock_get, mock_sleep):
    mock_get.side_effect = [
        fetch_fx.requests.exceptions.ConnectionError("connection reset"),
        _response(200, SUCCESS_BODY),
    ]
    rates = fetch_fx.fetch_rates_from_api()
    assert rates["TZS"] == 2600.0


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "test-key"})
@patch("fetch_fx.time.sleep")
@patch("fetch_fx.requests.get")
def test_gives_up_after_max_attempts(mock_get, mock_sleep):
    mock_get.return_value = _response(503, text="still down")
    with pytest.raises(fetch_fx.FxFetchError, match="Giving up after"):
        fetch_fx.fetch_rates_from_api()
    assert mock_get.call_count == fetch_fx.MAX_ATTEMPTS


@patch.dict("os.environ", {"EXCHANGERATE_API_KEY": "bad-key"})
@patch("fetch_fx.requests.get")
def test_non_retryable_error_fails_immediately(mock_get):
    # e.g. 403 invalid-key -- not a transient failure, retrying won't help
    mock_get.return_value = _response(403, text="invalid-key")
    with pytest.raises(fetch_fx.FxFetchError, match="403"):
        fetch_fx.fetch_rates_from_api()
    assert mock_get.call_count == 1
