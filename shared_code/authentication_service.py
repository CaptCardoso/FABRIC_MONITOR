import json
import logging
#from fabric_monitor.shared_code.fabric_monitor_infra import send_api_request
from fabric_monitor.shared_code.utils.helpers import setup_logging, config_requests

# Create logger
logger = logging.getLogger('AuthenticationService')
# Instantiate requests session
r = config_requests(retry_limit=5, retry_delay=2)

class AuthService:
    """
    A class that handles authentication for accessing the Microsoft APIs.
    """

    def __init__(self, tenant_id, client_id, client_secret, resource):
        """
        Initializes the AuthService with the provided credentials.

        Args:
            tenant_id (str): The ID of the Azure Active Directory tenant.
            client_id (str): The client ID of the registered application.
            client_secret (str): The client secret of the registered application.
        """
        self.base_url = 'https://login.microsoftonline.com'
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource = resource
        self.grant_type = 'client_credentials'


    def get_auth_token(self):
        """
        Retrieves an access token for accessing the Power BI API.

        Returns:
            str: The access token.

        Raises:
            Exception: If retrieval of the access token fails.
        """
        try:
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            auth_body = f'grant_type={self.grant_type}&resource={self.resource}&client_id={self.client_id}&client_secret={self.client_secret}'
            aad_client = r.post(url=f'{self.base_url}/{self.tenant_id}/oauth2/token', headers=headers, data=auth_body)
            aad_client = json.loads(aad_client.text)

            logger.info('Successfully retrieved access token.')

            return aad_client['access_token']

        except Exception as e:
            logger.error(f'Retrieval of access token failed: {e}')
            raise
