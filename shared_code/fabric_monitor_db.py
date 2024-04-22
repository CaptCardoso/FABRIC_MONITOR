import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('FabricMonitorDb')

class FabricMonitorDb:
    """Class representing the Fabric Monitor Database."""


    def __init__(self, project_id, dataset_id, bq_client):
        """
        Initializes a new instance of the FabricMonitorDb class.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            bq_client (google.cloud.bigquery.client.Client): An instance of the BigQuery client.

        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client
        self.create_and_populate_previous_scans_table()


    def create_dataset_if_not_exists(self):

        client = bigquery.Client(project=self.project_id)
        dataset_ref = client.dataset(self.dataset_id)

        try:
            client.get_dataset(dataset_ref)
            logger.info('Dataset {} already exists'.format(self.dataset_id))

        except NotFound:
            # Create the dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'US'
            dataset = client.create_dataset(dataset)  # Make an API request.
            logger.info('Created dataset {}.{}'.format(client.project, dataset.dataset_id))


    def create_table(self, table_full_path, schema):

        table = bigquery.Table(table_full_path, schema=schema)

        try:
            self.bq_client.create_table(table)
            logger.info(f'Table {table_full_path} created successfully.')

        except Exception as e:
            logger.error(f'Failed to create table: {e}')


    def create_and_populate_previous_scans_table(self, table_id='pbi_scan_status'):
        """
        Check if a BigQuery table exists, and if not, create it.

        Parameters:
        - table_id: BigQuery table ID.
        """

        try:
            # Create dataset if necessary
            self.create_dataset_if_not_exists()

            # Check if table already exists
            try:
                self.bq_client.get_table(f"{self.project_id}.{self.dataset_id}.{table_id}")
                logger.info(f'{table_id} table already exists.')

            except NotFound:
                table_full_path = f"{self.project_id}.{self.dataset_id}.{table_id}"

                schema = [
                    bigquery.SchemaField('scan_type', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('last_run_date', 'DATE'),
                    bigquery.SchemaField('last_full_scan_date', 'DATE')
                ]

                # Create table
                self.create_table(table_full_path=table_full_path, schema=schema)

                # Add rows
                insert_query = f'''
                    INSERT INTO {table_full_path} (scan_type)
                    VALUES ('activity'), ('catalog')
                '''
                self.bq_client.query(insert_query).result()
                logger.info(f'{table_id} table created and populated successfully.')

        except Exception as e:
            logger.error(f'{table_id} table creation failed: {e}')
            raise