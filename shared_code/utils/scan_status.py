import logging
from datetime import datetime
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('ScanStatusUtilities')

class ScanStatusUtilities:
    """
    A class that provides methods to interact with the scan_status table
    """

    def __init__(self, spark_session, project_id, dataset_id, bq_client):
        self.spark_session = spark_session
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client


    def get_last_scan_dates(self, scan_type):
        try:
            table_full_path = f'{self.project_id}.{self.dataset_id}.pbi_scan_status'
            pbi_scan_status = self.spark_session.read.format('bigquery').option('table', table_full_path).load()
            pbi_scan_status.createOrReplaceTempView('vw_pbi_scan_status')
            sql = f'''
                SELECT
                    DATE_FORMAT(COALESCE(last_run_date, '1990-01-01 00:00:00'), 'yyyy-MM-dd') AS most_recent_scan,
                    DATE_FORMAT(COALESCE(last_full_scan_date, '1990-01-01 00:00:00'), 'yyyy-MM-dd') AS most_recent_full_scan,
                    DATEDIFF(last_full_scan_date, last_run_date) AS days_since_last_full_scan
                FROM vw_pbi_scan_status
                WHERE scan_type = '{scan_type}'
            '''
            sql_result = self.spark_session.sql(sql).head()
            last_scan = sql_result.most_recent_scan
            last_full_scan = sql_result.most_recent_full_scan
            last_scan_datetime = datetime.strptime(last_scan, '%Y-%m-%d')
            last_full_scan_datetime = datetime.strptime(last_full_scan, '%Y-%m-%d')

            output = {
                'last_partial_scan': last_scan_datetime,
                'last_full_scan': last_full_scan_datetime
            }
            message = f'Last {scan_type} scan dates successfully retrieved.'
            logger.info(message)
            return output

        except Exception as e:
            message = f'Retrieval of last {scan_type} scan dates failed: {e}'
            logger.error(message)
            raise


    def get_last_scan_info(self, current_date, scan_type):
        try:
            last_scans = self.get_last_scan_dates(scan_type=scan_type)
            last_partial_scan_datetime = last_scans['last_partial_scan']
            last_full_scan_datetime = last_scans['last_full_scan']
            days_since_last_partial_scan = abs((current_date - last_partial_scan_datetime.date()).days)
            days_since_last_full_scan = abs((current_date - last_full_scan_datetime.date()).days)

            output = {
                'last_partial_scan': {
                    'days_since': days_since_last_partial_scan,
                    'last_scan_datetime': last_partial_scan_datetime
                },
                'last_full_scan': {
                    'days_since': days_since_last_full_scan,
                    'last_scan_datetime': last_full_scan_datetime
                }
            }

            message = f'Days since last {scan_type} and last scan date successfully retrieved.'
            logger.info(message)
            return output

        except Exception as e:
            message = f'Retrieval of days since last {scan_type} scan and last scan date failed: {e}'
            logger.error(message)
            raise


    def update_last_scan_date(self, scan_type='catalog', full_scan=False):

        try:
            if full_scan:
                sql = f'''
                    UPDATE {self.dataset_id}.pbi_scan_status
                    SET
                        last_run_date = current_date(),
                        last_full_scan_date = current_date()
                    WHERE scan_type = '{scan_type}'
                '''
            else:
                sql = f'''
                    UPDATE {self.dataset_id}.pbi_scan_status
                    SET
                        last_run_date = current_date()
                    WHERE scan_type = '{scan_type}'
                '''
            sql_result = self.bq_client.query(sql).result()

            if full_scan:
                message = 'Last_run_date and last_full_scan_date columns in pbi_scan_status updated to today.'
            else:
                message = 'Last_run_date column in pbi_scan_status updated to today.'
                logger.info(message)
            return True

        except Exception as e:
            message = f'Error updating pbi_scan_status: {e}'
            logger.error(message)
            raise