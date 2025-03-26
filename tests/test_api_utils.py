import pytest
from requests.exceptions import ConnectionError, Timeout, RequestException
from utils.api_utils import RestAdapter, MSGraphApi, TenoviApi


@pytest.fixture
def rest_adapter():
    return RestAdapter(base_url="https://api.example.com")


def test_get_request_success(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.get(
        f"https://api.example.com{endpoint}", json={"key": "value"}, status_code=200
    )
    response = rest_adapter.get(endpoint)
    assert response == {"key": "value"}


def test_post_request_success(rest_adapter, requests_mock):
    endpoint = "/test"
    data = {"data": "value"}
    requests_mock.post(
        f"https://api.example.com{endpoint}", json={"key": "value"}, status_code=200
    )
    response = rest_adapter.post(endpoint, data=data)
    assert response == {"key": "value"}


def test_put_request_success(rest_adapter, requests_mock):
    endpoint = "/test"
    data = {"data": "value"}
    requests_mock.put(
        f"https://api.example.com{endpoint}", json={"key": "value"}, status_code=200
    )
    response = rest_adapter.put(endpoint, data=data)
    assert response == {"key": "value"}


def test_delete_request_success(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.delete(
        f"https://api.example.com{endpoint}", json={"key": "value"}, status_code=200
    )
    response = rest_adapter.delete(endpoint)
    assert response == {"key": "value"}


def test_request_http_error(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.get(f"https://api.example.com{endpoint}", status_code=404)
    response = rest_adapter.get(endpoint)
    assert response is None


def test_request_connection_error(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.get(f"https://api.example.com{endpoint}", exc=ConnectionError)
    response = rest_adapter.get(endpoint)
    assert response is None


def test_request_timeout_error(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.get(f"https://api.example.com{endpoint}", exc=Timeout)
    response = rest_adapter.get(endpoint)
    assert response is None


def test_request_unexpected_error(rest_adapter, requests_mock):
    endpoint = "/test"
    requests_mock.get(f"https://api.example.com{endpoint}", exc=RequestException)
    response = rest_adapter.get(endpoint)
    assert response is None


@pytest.fixture
def ms_graph_api():
    return MSGraphApi(
        tenant_id="tenant_id", client_id="client_id", client_secret="client_secret"
    )


def test_request_access_token(ms_graph_api, requests_mock):
    token_endpoint = "https://login.microsoftonline.com/tenant_id/oauth2/v2.0/token"
    requests_mock.post(
        token_endpoint, json={"access_token": "test_token"}, status_code=200
    )
    ms_graph_api.request_access_token()
    assert ms_graph_api.rest.session.headers["Authorization"] == "Bearer test_token"


def test_get_group_members(ms_graph_api, requests_mock):
    ms_graph_api.request_access_token()
    group_id = "group_id"
    endpoint = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
    requests_mock.get(endpoint, json={"value": [{"id": "member_id"}]}, status_code=200)
    response = ms_graph_api.get_group_members(group_id)
    assert response == {"value": [{"id": "member_id"}]}


@pytest.fixture
def tenovi_api():
    return TenoviApi(client_domain="client_domain", api_key="api_key")


def test_get_devices(tenovi_api, requests_mock):
    endpoint = "https://api2.tenovi.com/clients/client_domain/hwi/hwi-devices"
    requests_mock.get(endpoint, json=[{"device_id": "device_1"}], status_code=200)
    response = tenovi_api.get_devices()
    assert response == [{"device_id": "device_1"}]


def test_get_readings(tenovi_api, requests_mock):
    hwi_device_id = "device_id"
    endpoint = f"https://api2.tenovi.com/clients/client_domain/hwi/hwi-devices/{hwi_device_id}/measurements/"
    requests_mock.get(endpoint, json=[{"reading_id": "reading_1"}], status_code=200)
    response = tenovi_api.get_readings(hwi_device_id)
    assert response == [{"reading_id": "reading_1"}]
