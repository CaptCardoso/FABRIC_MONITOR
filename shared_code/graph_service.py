import logging
#from fabric_monitor.shared_code.fabric_monitor_infra import send_api_request
from fabric_monitor.shared_code.utils.helpers import setup_logging, config_requests

# Add logging
logger = logging.getLogger('GraphService')
# Instantiate requests session
r = config_requests(retry_limit=5, retry_delay=2)

class GraphService:
    """
    A class that provides methods for interacting with the Graph API.

    Args:
        access_token (str): The access token for authentication.

    Attributes:
        base_url (str): The base URL for the Graph API.
        auth_header (dict): The authentication header for API requests.

    """

    def __init__(self, access_token, base_url='https://graph.microsoft.com/v1.0'):
        self.base_url = base_url
        self.auth_header = {'Authorization': f'Bearer {access_token}'}

    def get_graph_api_resource(self, resource, query_parameters=None):
        """
        Retrieves data from the requested Graph API resource using specified query parameters.

        Args:
            resource (string, required): The resource to be queried. (e.g. users, subscribedSkus)
            query_parameters (string, required): The query parameters to be used in the request (e.g. $select=id,displayName,assignedLicenses,UserPrincipalName)

        Returns:
            list: The data from the requested Graph API resource.
        """
        if query_parameters:
            url = f'{self.base_url}/{resource}?{query_parameters}'
        else:
            url = f'{self.base_url}/{resource}'
        result = r.get(url=url, headers=self.auth_header).json()
        output = result.get('value', [])
        while '@odata.nextLink' in result:
            result = r.get(url=result['@odata.nextLink'], headers=self.auth_header).json()
            output.extend(result.get('value', []))
        return output