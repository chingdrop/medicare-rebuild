import logging
import requests


class RestAdapter:
    def __init__(
            self,
            base_url,
            headers=None,
            auth=None,
            logger=logging.getLogger()
    ):
        """
        Initialize the RequestHandler instance.
        :param base_url: The base URL for the API
        :param headers: Default headers (optional)
        :param auth: Authentication information (optional)
        """
        self.base_url = base_url
        self.headers = headers if headers else {}
        self.auth = auth
        self.logger = logger

        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        if self.headers:
            self.session.headers.update(self.headers)

    def _send_request(self, method, endpoint, params=None, data=None):
        """
        Prepare the request to be sent. Send the prepared request and return the response.
        :param method: HTTP method ('GET', 'POST', etc.)
        :param endpoint: API endpoint (e.g., '/users', '/posts')
        :param params: URL parameters (optional)
        :param data: Data to send in the request body (optional)
        :return: Response object
        """
        self.logger.debug(f'Request [{method}] - {self.base_url} {endpoint}')
        url = f"{self.base_url}{endpoint}"
        req = requests.Request(method, url, headers=self.session.headers, params=params, data=data)
        prep_req = self.session.prepare_request(req)
        try:
            response = self.session.send(prep_req)
            response.raise_for_status()
            self.logger.debug(f'Status [{response.status_code}] - {response.reason}')
            if response:
                return response.json()
        except requests.exceptions.HTTPError as errh:
            self.logger.error(f"HTTP Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            self.logger.error(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            self.logger.error(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            self.logger.error(f"An Unexpected Error: {err}")

    def get(self, endpoint, params=None):
        """
        Make a GET request.
        :param endpoint: API endpoint
        :param params: URL parameters (optional)
        :return: JSON response or None if an error occurs
        """
        return self._send_request('GET', endpoint, params=params)

    def post(self, endpoint, data=None):
        """
        Make a POST request.
        :param endpoint: API endpoint
        :param data: Data to send in the request body
        :return: JSON response or None if an error occurs
        """
        return self._send_request('POST', endpoint, data=data)

    def put(self, endpoint, data=None):
        """
        Make a PUT request.
        :param endpoint: API endpoint
        :param data: Data to send in the request body
        :return: JSON response or None if an error occurs
        """
        return self._send_request('PUT', endpoint, data=data)

    def delete(self, endpoint, params=None):
        """
        Make a DELETE request.
        :param endpoint: API endpoint
        :param params: URL parameters (optional)
        :return: JSON response or None if an error occurs
        """
        return self._send_request('DELETE', endpoint, params=params)


class MSGraphApi:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id,
        self.client_secret = client_secret

    def request_access_token(self,):
        rest = RestAdapter('https://login.microsoftonline.com')
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        res = rest.post(f'/{self.tenant_id}/oauth2/v2.0/token', data=data)
        access_token = res.get('access_token')
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        self.rest = RestAdapter('https://graph.microsoft.com/v1.0', headers=headers)

    def get_group_members(self, group_id):
        endpoint = f'/groups/{group_id}/members'
        return self.rest.get(endpoint)
