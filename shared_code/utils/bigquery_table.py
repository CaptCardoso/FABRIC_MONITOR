import logging
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('BigQueryUtilities')

class BigQueryUtilities:
    """
    A class that provides methods create and modify BigQuery tables.
    """

    def __init__(self, spark_session, project_id, dataset_id, bq_client):
        self.spark_session = spark_session
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client


    def write_to_bigquery(self, df, table_id, quietly=False):
        """
        Writes the specified DataFrame to a BigQuery table, overwriting any existing data.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to write to BQ.
            table_id (str): The table ID/name of the target BQ table.

            Returns:
                bool: True if the write operation is successful, False otherwise.
            """

        table_full_path = f'{self.project_id}.{self.dataset_id}.{table_id}'

        try:
            df.write.format('bigquery') \
                .option('table', table_full_path) \
                .option('temporaryGcsBucket', 'fabric-monitor-data') \
                .mode('overwrite') \
                .save()
            if not quietly:
                message = f'Data written to {table_full_path} successfully.'
                logger.info(message)

            return True

        except Exception as e:
            message = f'Error writing data to {table_full_path}: {e}'
            logger.error(message)
            raise


    def delete_then_insert_in_memory(self, target_table_id, source_table_df, merge_keys):
        """
        Deletes rows from the target table that match the merge keys and inserts the rows from the source table into the target table in memory.

        Args:
            target_table_id (str): The ID/name of the target BQ table.
            source_table_df (DataFrame): The DataFrame containing the rows to be inserted.
            merge_keys (list): The list of column names used to match rows for deletion.

        Returns:
            DataFrame: The resulting DataFrame after the deletion and insertion.

        Raises:
            Exception: If there is an error updating the target table in memory.
        """

        table_full_path = f'{self.project_id}.{self.dataset_id}.{target_table_id}'

        try:
            target_df = self.spark_session.read.format('bigquery').option('table', table_full_path).load()
            output_df = target_df.join(source_table_df, on=merge_keys, how='left_anti').unionByName(source_table_df, allowMissingColumns=True)
            self.write_to_bigquery(df=output_df, table_id=target_table_id, quietly=True)
            message = f'Successfully updated {table_full_path} in memory and wrote to BigQuery.'
            logger.info(message)
            return output_df

        except Exception as e:
            message = f'Error updating {table_full_path} in memory: {e}'
            logger.error(message)
            raise


    def update_bigquery_table(self, table_id, parameters):
        """
        Overwrites existing data or merges with new data'.

        Args:
            table_id (str): The ID/name of the BigQuery table to optimize.

            parameters (dict): A dictionary containing the following key-value pairs:
                df (DataFrame): The DataFrame to write to BQ.
                merge_keys (list): The list of column names used to match rows for deletion.
                merge_type (str): The type of merge operation to perform.

        Returns:
            bool: True if the write is successful.
        """
        table_full_path = f'{self.project_id}.{self.dataset_id}.{table_id}'

        try: #Is this really a good replacement for if/else?
            # Attempt to fetch the BigQuery table
            self.bq_client.get_table(table_full_path)
            self.delete_then_insert_in_memory(
                target_table_id=table_id,
                source_table_df=parameters['df'],
                merge_keys=parameters['merge_keys']
            )
        except:
            self.write_to_bigquery(df=parameters['df'], table_id=table_id)

        return True