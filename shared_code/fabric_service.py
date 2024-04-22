import json
import time
import logging
# import sempy.fabric as fabric
from fabric_monitor.shared_code.utils.helpers import setup_logging, config_requests

# Create logger
# logger = logging.getLogger('FabricService')
# Instantiate requests session
r = config_requests(retry_limit=5, retry_delay=2)

class FabricService:
    """
    A class that provides methods for interacting with the Power BI API.

    Args:
        organization (str): The name of the organization.
        access_token (str): The access token for authentication.

    Attributes:
        base_url (str): The base URL for the Power BI API.
        auth_header (dict): The authentication header for API requests.

    """

    def __init__(self, organization, access_token):
        self.base_url = f"https://api.powerbi.com/v1.0/{organization}/admin"
        self.auth_header = {'Authorization': f'Bearer {access_token}'}

    def create_chunks(self, lst, n):
        """
        Splits a list into chunks of size n. This is required for the get_modified_workspaces
        function which can accept a max of 100 workspaces per API call.

        Args:
            lst (list): The list to be split.
            n (int): The size of each chunk.

        Yields:
            list: A chunk of the original list.

        """
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_modified_workspaces(self, last_updated_datetime=None, exclude_personal_workspaces=False, exclude_inactive_workspaces=True):
        """
        Retrieves the modified workspaces from the Power BI API.

        Args:
            last_updated_datetime (datetime, optional): The datetime to filter the workspaces. Defaults to None.
            exclude_personal_workspaces (bool, optional): Whether to exclude personal workspaces. Defaults to False.
            exclude_inactive_workspaces (bool, optional): Whether to exclude inactive workspaces. Defaults to True.

        Returns:
            dict: The modified workspaces.

        """
        url = f"{self.base_url}/workspaces/modified?excludePersonalWorkspaces={exclude_personal_workspaces}&excludeInactiveWorkspaces={exclude_inactive_workspaces}"
        if last_updated_datetime:
            url = f"{url}&modifiedSince={last_updated_datetime.strftime('%Y-%m-%dT%H:%M:%S.0000000Z')}"

        get_modified_workspaces = r.get(url=url, headers=self.auth_header)
        modified_workspaces = json.loads(get_modified_workspaces.text)
        return modified_workspaces

    def post_scan_workspaces(self, body, lineage=True, datasourceDetails=True, datasetExpressions=True, datasetSchema=True, getArtifactUsers=True):
        """
        Posts a request to scan workspaces in the Power BI API.

        Args:
            body (dict): The request body.
            lineage (bool, optional): Whether to include lineage. Defaults to True.
            datasourceDetails (bool, optional): Whether to include datasource details. Defaults to True.
            datasetExpressions (bool, optional): Whether to include dataset expressions. Defaults to True.
            datasetSchema (bool, optional): Whether to include dataset schema. Defaults to True.
            getArtifactUsers (bool, optional): Whether to include artifact users. Defaults to True.

        Returns:
            dict: The scan response.

        """
        url  = f"{self.base_url}/workspaces/getInfo?lineage={lineage}&datasourceDetails={datasourceDetails}&datasetExpressions={datasetExpressions}&datasetSchema={datasetSchema}&getArtifactUsers={getArtifactUsers}"
        workspaces_scan = r.post(url=url, headers=self.auth_header, data=body)
        scan_response = json.loads(workspaces_scan.text)
        return scan_response

    def get_scan_status(self, id):
        """
        Retrieves the scan status of a workspace from the Power BI API.

        Args:
            id (str): The ID of the workspace.

        Returns:
            dict: The scan status.

        """
        url = f"{self.base_url}/workspaces/scanStatus/{id}"
        get_scan_status = r.get(url=url, headers=self.auth_header)
        scan_status = json.loads(get_scan_status.text)
        return scan_status

    def get_scan_result(self, id, seconds=5):
        """
        Retrieves the scan result of a workspace from the Power BI API.

        Args:
            id (str): The ID of the workspace.
            seconds (int, optional): The interval in seconds to check the scan status. Defaults to 5.

        Returns:
            dict: The scan result.

        """
        url = f"{self.base_url}/workspaces/scanResult/{id}"
        scan_status = {}
        attempt = 0
        while scan_status.get('status', '') != 'Succeeded':
            attempt += 1
            time.sleep(seconds)
            scan_status = self.get_scan_status(id)
        get_scan_result = r.get(url=url, headers=self.auth_header)
        scan_result = json.loads(get_scan_result.text)
        return scan_result

    def get_activity_events(self, startDateTime, endDateTime, c_token=None):
        """
        Retrieves the activity events from the Power BI API.

        Args:
            startDateTime (datetime): The start datetime of the events.
            endDateTime (datetime): The end datetime of the events.
            c_token (str, optional): The continuation token for pagination. Defaults to None.

        Returns:
            tuple: A tuple containing the events and the continuation token.

        """
        url = f"{self.base_url}/activityevents"
        if c_token != None:
            url = f"{url}?continuationToken='{c_token}'"
        else:
            url = f"{url}?startDateTime='{startDateTime.strftime('%Y-%m-%dT%H:%M:%S.000Z')}'&endDateTime='{endDateTime.strftime('%Y-%m-%dT%H:%M:%S.999Z')}'"

        r_activity_events = r.get(url=url, headers=self.auth_header)
        activity_events = json.loads(r_activity_events.text)
        events = activity_events['activityEventEntities']
        c_token = activity_events['continuationToken']
        return events, c_token

    def get_power_bi_api_resource(self, resource, query_parameters=None, base_url_override=None):
        """
        Retrieves data from the requested Power BI API resource using specified query parameters.

        Args:
            resource (string, required): The resource to be queried. (e.g. apps, capacities, capacities/refreshables)
            query_parameters (string, optional): The query parameters to be used in the request (e.g. $top=5000). Include $ in parameter string.
            base_url_override (string, optional): The base URL to be used in the request. Defaults to None.

        Returns:
            list: The data from the requested Power BI API resource.

        Example Usage:
            get_power_bi_api_resource(resource='capacities')
            get_power_bi_api_resource(resource='capacities/refreshables')
            get_power_bi_api_resource(resource='apps', query_parameters='$top=5000')
            get_power_bi_api_resource(resource='tenantsettings', base_url_override='https://api.powerbi.com/v1/admin')
        """

        key = 'tenantSettings' if resource=='tenantsettings' else 'value'
        base_url = base_url_override if base_url_override else self.base_url
        if query_parameters:
            url = f'{base_url}/{resource}?{query_parameters}'
        else:
            url = f'{base_url}/{resource}'
        response = r.get(url=url, headers=self.auth_header)
        return json.loads(response.text)[key]


    # @staticmethod
    # def refresh_dataset(dataset):
    #     """
    #     Refreshes a semantic model via the Semantic Link python package which is a wrapper over the Power BI API.

    #     Args:
    #         dataset (string, required): The name of the semantic model to refresh.
    #     """
    #     request_status_id = fabric.refresh_dataset(dataset=dataset)
    #     print(f'{dataset} Progress:', end='')
    #     while True:
    #         status = fabric.get_refresh_execution_details(dataset, request_status_id).status
    #         if status == 'Completed':
    #             break
    #         print('░', end='')
    #         time.sleep(15)
    #     print(": refresh complete")
