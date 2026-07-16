import pytest
import requests

from medicare_rebuild.utils.api_utils import MSGraphApi, TenoviApi

JSON_HEADERS = {"Content-Type": "application/json"}


@pytest.fixture
def ms_graph_api():
    return MSGraphApi(
        tenant_id="tenant_id", client_id="client_id", client_secret="client_secret"
    )


def test_request_access_token(ms_graph_api, requests_mock):
    token_endpoint = "https://login.microsoftonline.com/tenant_id/oauth2/v2.0/token"
    requests_mock.post(
        token_endpoint,
        json={"access_token": "test_token"},
        headers=JSON_HEADERS,
        status_code=200,
    )
    ms_graph_api.request_access_token()
    assert ms_graph_api.rest.session.headers["Authorization"] == "Bearer test_token"


def test_get_group_members(ms_graph_api, requests_mock):
    token_endpoint = "https://login.microsoftonline.com/tenant_id/oauth2/v2.0/token"
    requests_mock.post(
        token_endpoint,
        json={"access_token": "test_token"},
        headers=JSON_HEADERS,
        status_code=200,
    )
    ms_graph_api.request_access_token()
    group_id = "group_id"
    endpoint = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
    requests_mock.get(
        endpoint,
        json={"value": [{"id": "member_id"}]},
        headers=JSON_HEADERS,
        status_code=200,
    )
    response = ms_graph_api.get_group_members(group_id)
    assert response == {"value": [{"id": "member_id"}]}


def test_get_group_members_raises_on_http_error(ms_graph_api, requests_mock):
    token_endpoint = "https://login.microsoftonline.com/tenant_id/oauth2/v2.0/token"
    requests_mock.post(
        token_endpoint,
        json={"access_token": "test_token"},
        headers=JSON_HEADERS,
        status_code=200,
    )
    ms_graph_api.request_access_token()
    group_id = "group_id"
    endpoint = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
    requests_mock.get(endpoint, status_code=500)
    with pytest.raises(requests.HTTPError):
        ms_graph_api.get_group_members(group_id)


@pytest.fixture
def tenovi_api():
    return TenoviApi(client_domain="client_domain", api_key="api_key")


def test_get_devices(tenovi_api, requests_mock):
    endpoint = "https://api2.tenovi.com/clients/client_domain/hwi/hwi-devices"
    requests_mock.get(
        endpoint,
        json=[{"device_id": "device_1"}],
        headers=JSON_HEADERS,
        status_code=200,
    )
    response = tenovi_api.get_devices()
    assert response == [{"device_id": "device_1"}]


def test_get_readings(tenovi_api, requests_mock):
    hwi_device_id = "device_id"
    endpoint = f"https://api2.tenovi.com/clients/client_domain/hwi/hwi-devices/{hwi_device_id}/measurements/"
    requests_mock.get(
        endpoint,
        json=[{"reading_id": "reading_1"}],
        headers=JSON_HEADERS,
        status_code=200,
    )
    response = tenovi_api.get_readings(hwi_device_id)
    assert response == [{"reading_id": "reading_1"}]


def test_get_readings_raises_on_http_error(tenovi_api, requests_mock):
    hwi_device_id = "device_id"
    endpoint = f"https://api2.tenovi.com/clients/client_domain/hwi/hwi-devices/{hwi_device_id}/measurements/"
    requests_mock.get(endpoint, status_code=503)
    with pytest.raises(requests.HTTPError):
        tenovi_api.get_readings(hwi_device_id)
