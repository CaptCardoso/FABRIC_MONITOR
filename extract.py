import os
import json
import pytz
import logging
from datetime import datetime, timedelta
from fabric_monitor.shared_code.utils.helpers import setup_logging, check_if_path_exists
from google.cloud import storage

# Create logger
logger = logging.getLogger('ExtractionService')


class ExtractionService:
    """
    A class that handles extraction of data from the Power BI scanner APIs, Power BI Admin API, and Graph API.

    Attributes:
        fabric_service: An instance of the FabricService class.
        graph_service: An instance of the GraphService class.
        fabric_monitor_service: An instance of the FabricMonitorService class.
        fabric_monitor_db: An instance of the FabricMonitorDb class.
    """

    def __init__(self, fabric_service, graph_service, fabric_monitor_service, fabric_monitor_db, scan_status_utilities, bucket_name, storage_client):
        self.fabric_service = fabric_service
        self.graph_service = graph_service
        self.fabric_monitor_service = fabric_monitor_service
        self.fabric_monitor_db = fabric_monitor_db
        self.scan_status_utilities = scan_status_utilities
        self.current_date = datetime.utcnow().date()
        self.bucket_name = bucket_name
        self.storage_client = storage_client


    def write_to_file(self, data, file_path, quietly=False):
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(file_path)
            blob.upload_from_string(data, content_type='application/json')
            if not quietly:
                logger.info(f'Successfully saved file to {file_path}')
        except Exception as e:
            message = f'Error writing to {file_path}: {e}'
            logger.error(message)
            raise


    def extract_catalog_data(self, output_base_path, full_scan=False):
        """
        This function uses the Power BI Scanner API to extract metadata for all workspaces in the organization.
        The data is extract by each workspace in batches for 100.

        Args:
            output_base_path (str): The base path for the output files.
            full_scan (bool, optional): Whether to perform a full scan. Defaults to False. When false, the pbi_scan_status table is queried for
            the most recent scan_date and only incremental data is returned. A full scan is required if the last scan was more than 30 days ago.

        Output:
            Files saved as JSON to the lakehouse.
        """
        try:
            output_file_path = f'{output_base_path}/{self.current_date.year}/{self.current_date.strftime("%m")}/{self.current_date.strftime("%d")}'

            # Get data of last scan and last full scan
            last_scan_info = self.scan_status_utilities.get_last_scan_info(self.current_date, 'catalog')

            full_scan_required = full_scan
            partial_scan_required = False

            if last_scan_info['last_full_scan']['days_since'] > 30 or full_scan:
                full_scan_required = True

            if last_scan_info['last_partial_scan']['days_since'] > 0:
                partial_scan_required = True

            # Get all workspaces if a full scan is required
            if full_scan_required:
                message = f'Getting all workspaces...'
                logger.info(message)
                modified_workspaces = self.fabric_service.get_modified_workspaces()

            # Get workspaces updated since last scan if a partial scan is required
            elif partial_scan_required:
                last_partial_scan_datetime = last_scan_info['last_partial_scan']['last_scan_datetime']
                message = f'Getting workspaces updated since {last_partial_scan_datetime}...'
                logger.info(message)
                modified_workspaces = self.fabric_service.get_modified_workspaces(last_partial_scan_datetime)

            # Scan all identified workspaces
            if full_scan_required or partial_scan_required:

                # If output path exists, delete and recreate. If path does not exist, create.
                check_if_path_exists(output_file_path)
                check_if_path_exists(output_file_path.replace('unprocessed', 'processed'))

                message = f'Found {len(modified_workspaces)} workspaces to scan.'
                logger.info(message)
                number_of_workspaces_per_scan = 100
                workspace_chunks = self.fabric_service.create_chunks(modified_workspaces, number_of_workspaces_per_scan)
                number_of_scans = 0
                for workspace_chunk in workspace_chunks:
                    payload = {"workspaces": [workspace['id'] for workspace in workspace_chunk]}
                    scan_response = self.fabric_service.post_scan_workspaces(payload)
                    scan_response_id = scan_response['id']
                    scan_result = self.fabric_service.get_scan_result(scan_response_id)

                    scan_result_path = f'{output_file_path}/{scan_response_id}.json'

                    json_data = json.dumps(scan_result)

                    message = f'Writing scan {number_of_scans + 1} of {int(len(modified_workspaces)/number_of_workspaces_per_scan) + 1} scans to {scan_result_path}...'
                    logger.info(message)

                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(scan_result_path)
                    blob.upload_from_string(json_data, content_type='application/json')

                    number_of_scans += 1

                message = f'{number_of_scans} metadata scans successfully saved to {output_file_path}'

                self.scan_status_utilities.update_last_scan_date(scan_type='catalog',full_scan=full_scan_required)
            else:
                message = f'No metadata scans to extract; last scan was today.'
            logger.info(message)

            return True

        except Exception as e:
            message = f'Error extracting catalog data: {e}'
            logger.error(message)
            raise


    def extract_activity_data(self, output_base_path, limit_days_for_debugging=False):
        """
        This function uses the Power BI Admin ActivityEvents API to extract activity data within the tenant

        Args:
            output_base_path (str): The base path for the output files.
            limit_days_for_debugging (bool, optional): Whether to limit the number of days to extract for debugging purposes. Defaults to False.
            When true, only 3 days of data will be extracted which improves processing time.

        Output:
            Files saved as JSON to the lakehouse.
        """
        try:
            # Get most recent CreatedDate from PBI_Usage table
            last_scan_info = self.scan_status_utilities.get_last_scan_info(self.current_date, 'activity')
            days_since_last_scan = last_scan_info['last_partial_scan']['days_since']
            if days_since_last_scan > 30:
                days_since_last_scan = 30
            if limit_days_for_debugging:
                days_since_last_scan = 3

            if days_since_last_scan > 0:
                # Get activity events one day at a time
                for i in range(1, days_since_last_scan + 1):
                    startDateTime = datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(i)
                    endDateTime = datetime.now(pytz.utc).replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(i)

                    startDateTime_string = startDateTime.date().strftime("%Y%m%d")
                    message = f'Getting activity events for {startDateTime.date()}...'
                    logger.info(message)

                    output_file_path = f'{output_base_path}/{self.current_date.year}/{self.current_date.strftime("%m")}/{self.current_date.strftime("%d")}'

                    # If path exists, delete and recreate. If path does not exist, create.
                    check_if_path_exists(output_file_path)
                    check_if_path_exists(output_file_path.replace('unprocessed', 'processed'))

                    number_of_activities = 0
                    events, c_token = self.fabric_service.get_activity_events(startDateTime, endDateTime)

                    # Each day typically has multiple JSON files worth of activity. Pagination is required to get them all.
                    pagination = 0
                    while c_token is not None:

                        result_path = f'{output_file_path}/{startDateTime_string}.json'

                        if pagination > 0:
                            result_path = result_path.replace(f'{startDateTime_string}.json', f'{startDateTime_string}_{pagination}.json')

                        json_data = json.dumps(events)

                        self.write_to_file(json_data, result_path, quietly=True)

                        pagination += 1
                        number_of_activities += len(events)
                        events, c_token = self.fabric_service.get_activity_events(startDateTime, endDateTime, c_token)

                    message = f'Wrote {number_of_activities} activities for {startDateTime.date()}.'
                    logger.info(message)

                message = 'Activity data successfully extracted.'
                logger.info(message)
                self.scan_status_utilities.update_last_scan_date(scan_type='activity')

            else:
                message = 'No activity data to extract; last scan was today.'
                logger.info(message)

            return True

        except Exception as e:
            message = f'Error extracting activity data: {e}'
            logger.error(message)
            raise


    def extract_power_bi_admin_api_data(self, output_base_path='Files/Tenant Data'):
        """
        This function uses the Power BI Admin API to extract tenant-level data that is not included in the workspace-level metadata
        provided by the Scanner API (i.e. the "extract_catalog_data" function).
        )
        Args:
            output_base_path (str): The base path for the output files.

        Output:
            Files saved as JSON to the lakehouse.
        """
        try:
            capacities_json = json.dumps(self.fabric_service.get_power_bi_api_resource('capacities'))
            refreshables_json = json.dumps(self.fabric_service.get_power_bi_api_resource('capacities/refreshables'))
            apps_json = json.dumps(self.fabric_service.get_power_bi_api_resource(resource='apps', query_parameters='$top=5000'))
            tenantsettings_json = json.dumps(self.fabric_service.get_power_bi_api_resource(resource='tenantsettings', base_url_override='https://api.powerbi.com/v1/admin'))

            # Confirm Tenant Data directory exists
            os.makedirs(output_base_path, exist_ok=True) # output_base_path has to be the "File API" path since pure Python (not Spark) is used to write the files

            self.write_to_file(capacities_json, f'{output_base_path}/capacities.json')
            self.write_to_file(refreshables_json, f'{output_base_path}/refreshables.json')
            self.write_to_file(apps_json, f'{output_base_path}/apps.json')
            self.write_to_file(tenantsettings_json, f'{output_base_path}/tenant_settings.json')

            return True
        except Exception as e:
            message = f'Error extracting Power BI Admin API data: {e}'
            logger.error(message)
            raise


    def extract_graph_api_data(self, output_base_path='Files/Graph Data'):
        """
        This function uses the Graph API to extract non-Power BI specific data from the tenant such as user profiles and license information.

        Args:
            output_base_path (str): The base path for the output files.

        Output:
            Files saved as JSON to the lakehouse.
        """
        try:
            users_json = json.dumps(self.graph_service.get_graph_api_resource(resource='users', query_parameters='$select=id,displayName,assignedLicenses,UserPrincipalName'))
            subscribed_skus_json = json.dumps(self.graph_service.get_graph_api_resource(resource='subscribedSkus', query_parameters='$select=id,capabilityStatus,skuid,skupartnumber'))

            # Confirm Graph Data directory exists
            os.makedirs(output_base_path, exist_ok=True) # output_base_path has to be the "File API" path since pure Python (not Spark) is used to write the files

            self.write_to_file(users_json, f'{output_base_path}/users.json')
            self.write_to_file(subscribed_skus_json, f'{output_base_path}/subscribed_skus.json')

            return True
        except Exception as e:
            message = f'Error extracting Graph API data: {e}'
            logger.error(message)
            raise