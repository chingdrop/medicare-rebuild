import logging
from datetime import datetime

from shared_tools.rest_adapter import RestAdapter, RestAdapterConfig


class MSGraphApi:
    """
    Class to interact with Microsoft Graph API.

    Args:
        tenant_id (str): The tenant ID for the Azure AD application.
        client_id (str): The client ID for the Azure AD application.
        client_secret (str): The client secret for the Azure AD application.
        logger (Logger): Custom logger object (optional).
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.tenant_id = tenant_id
        self.client_id = (client_id,)
        self.client_secret = client_secret

    def request_access_token(
        self,
    ) -> None:
        """
        Uses tenant ID, client ID, and client secret to request an access token with privileges outlined in the application object.
        """
        rest = RestAdapter(
            RestAdapterConfig(base_url="https://login.microsoftonline.com/"),
            logger=self.logger,
        )
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }
        res = rest.post(f"{self.tenant_id}/oauth2/v2.0/token", data=data)
        assert isinstance(res, dict), "Expected a JSON object from the token endpoint"
        access_token = res.get("access_token")
        headers = {"Authorization": f"Bearer {access_token}"}
        self.rest = RestAdapter(
            RestAdapterConfig(
                base_url="https://graph.microsoft.com/v1.0/", headers=headers
            ),
            logger=self.logger,
        )

    def get_group_members(self, group_id: str) -> dict | list | str | bytes:
        """
        Get all members that belong to a specific group.

        Args:
            group_id (str): GUID of the desired group.

        Returns:
            dict: JSON serialized response body.
        """
        endpoint = f"groups/{group_id}/members"
        return self.rest.get(endpoint)


class TenoviApi:
    """
    Class to interact with Tenovi API.

    Args:
        client_domain (str): The client domain for the Tenovi API.
        api_key (str): The API key for authentication.
        logger (Logger): Custom logger object (optional).
    """

    def __init__(self, client_domain: str, api_key: str, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        headers = {"Authorization": f"Api-Key {api_key}"}
        self.rest = RestAdapter(
            RestAdapterConfig(
                base_url=f"https://api2.tenovi.com/clients/{client_domain}/",
                headers=headers,
            ),
            logger=self.logger,
        )

    def get_devices(
        self,
    ) -> dict | list | str | bytes:
        """
        Get a list of devices.

        Returns:
            List[dict]: List of devices.
        """
        return self.rest.get("hwi/hwi-devices")

    def get_readings(
        self,
        hwi_device_id: str,
        metric: str = "",
        created_gte: datetime | str | None = None,
    ) -> dict | list | str | bytes:
        """
        Get readings for a specific device.

        Args:
            hwi_device_id (str): The hardware ID of the device.
            metric (str): The name of the metric data to filter by (optional).
            created_gte (datetime, str): The earliest creation date to filter by (optional).

        Returns:
            List[dict]: List of readings.
        """
        params = {}
        if metric:
            params["metric__name"] = metric
        if created_gte:
            if not isinstance(created_gte, str):
                created_gte = created_gte.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["created__gte"] = created_gte
        return self.rest.get(
            f"hwi/hwi-devices/{hwi_device_id}/measurements/", params=params
        )
