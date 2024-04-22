import logging
from pyspark.sql.functions import expr
from fabric_monitor.shared_code.fabric_monitor_udfs import FabricMonitorUDFs
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('ParsingService')

class ParsingService:
    """
    A class that handles parsing raw M-query string to extract data element.

    Attributes:
        spark_session: The SparkSession object.
        BigQuery_table_utilities: An instance of the BigQueryUtilities class.

    This class is still in development and the code may not be optimized or follow best practices. Additionally,
    the parsing logic currently only extracts connection info for a subset of data sources including (but not limited to) Snowflake,
    Databricks, and SQL. However, it is not perfect and fails to extract connection info in some scenarios. The logic needs to be
    refined and extended to include other data sources.
    """

    def __init__(self, spark_session, BigQuery_table_utilities):
        self.spark_session = spark_session
        self.BigQuery_table_utilities = BigQuery_table_utilities


    def parse_connection_information(self):
        """
        Parses the raw M-query string to extract data elements.

        Args:
            None

        Returns:
            DataFrame: The resulting DataFrame containing the parsed data elements.
        """
        try:
            logger.info('Parsing connection information...')
            # Get table source expression information
            sql = '''
                SELECT
                    ws.workspace_name, ds.dataset_name, ds.dataset_id, ds.dataset_contentProviderType AS dataset_type, ts.table_name,
                    CASE
                        WHEN ts.source_expression LIKE '%Databricks.Catalog%' THEN 'Databricks'
                        WHEN ts.source_expression LIKE '%Snowflake.%' THEN 'Snowflake'
                    END AS source_type,
                    ts.source_expression
                FROM
                    FabricMonitor.pbi_table_sources AS ts
                INNER JOIN
                    FabricMonitor.pbi_datasets AS ds
                    ON
                        ts.dataset_id = ds.dataset_id
                INNER JOIN
                    FabricMonitor.pbi_workspaces AS ws
                    ON
                        ts.workspace_id = ws.workspace_id
            '''

            # Get db/schema/object
            source_expressions = self.spark_session.sql(sql)
            source_expressions = source_expressions.withColumn('source_expression_extracted', FabricMonitorUDFs.extract_database_source_object(source_expressions.source_expression))
            source_expressions = (source_expressions
                .withColumn('source_database', source_expressions.source_expression_extracted.Database)
                .withColumn('source_schema', source_expressions.source_expression_extracted.Schema)
                .withColumn('source_object', source_expressions.source_expression_extracted.Object)
                #.withColumn('source_type', source_expressions.source_expression_extracted.Kind)
                .drop('source_expression_extracted')
                .sort('source_type')
            )

            # Get connection type
            source_expressions = source_expressions.withColumn('connection_type_extracted', FabricMonitorUDFs.extract_mquery_connection_type(source_expressions.source_expression))
            sql = '''
                CASE
                    WHEN table_name = source_expression THEN 'Live Connection'
                    WHEN source_expression NOT LIKE 'let%' THEN 'Calculated Table'
                    ELSE connection_type_extracted
                END
            '''
            source_expressions = source_expressions.withColumn('connection_type', expr(sql))
            source_expressions.createOrReplaceTempView('vw_dataset_sources')
            logger.info('Connection information parsed successfully.')

            return True

        except Exception as e:
            message = f'Error parsing connection information: {e}'
            logger.error(message)
            raise


    def parse_dataset_expressions(self):
        """
        Parse data source expression values into columns. Expressions are M-query expressions that do not directly produce a
        PBI query (i.e. "Enabled Load" option = False in PBI desk). They include logic for intermediate steps
        as well as parameter info.

        Args:
            None

        Returns:
            DataFrame: The resulting DataFrame containing the parsed data elements.
        """
        try:
            # Get table source expression information
            logger.info('Parsing dataset expressions...')
            sql = '''
                SELECT
                    ds_exp.*
                FROM
                    FabricMonitor.pbi_dataset_expressions AS ds_exp
                INNER JOIN
                    FabricMonitor.pbi_datasets AS ds
                    ON
                        ds_exp.dataset_id = ds.dataset_id
            '''

            dataset_expressions = self.spark_session.sql(sql)
            dataset_expressions = dataset_expressions.withColumn('parameter_details', FabricMonitorUDFs.extract_mquery_parameter_details(dataset_expressions.dataset_expression))
            dataset_expressions = (dataset_expressions
                .withColumn('value', dataset_expressions.parameter_details.Value)
                .withColumn('is_parameter_query', dataset_expressions.parameter_details.IsParameterQuery)
                .withColumn('type', dataset_expressions.parameter_details.Type)
                .withColumn('is_parameter_query_required', dataset_expressions.parameter_details.IsParameterQueryRequired)
                .drop('parameter_details')
            )

            dataset_expressions.createOrReplaceTempView('vw_dataset_expressions')
            logger.info('Dataset expressions parsed successfully.')

            return True

        except Exception as e:
            message = f'Error parsing dataset expressions: {e}'
            logger.error(message)
            raise


    def update_data_source_parameters_and_write_to_table(self):
        # Update database and schema parameters with their actual values, write to 'pbi_dataset_datasources_parsed' for analysis/sharing

        try:
            logger.info('Updating data source parameters...')
            sql = '''
                SELECT DISTINCT
                    p.workspace_name, p.dataset_name, p.dataset_id, p.dataset_type, p.table_name,
                    p.connection_type,
                    COALESCE(exp_database.value, p.source_database) AS database,
                    COALESCE(exp_schema.value, p.source_schema) AS schema,
                    p.source_object AS object,
                    p.source_expression AS raw_source_expression
                FROM
                    vw_dataset_sources AS p
                LEFT JOIN
                    vw_dataset_expressions AS exp_database
                    ON
                        p.dataset_id = exp_database.dataset_id
                        AND REPLACE(p.source_database, '#', '') = exp_database.dataset_expression_name
                LEFT JOIN
                    vw_dataset_expressions AS exp_schema
                    ON
                        p.dataset_id = exp_schema.dataset_id
                        AND REPLACE(p.source_schema, '#', '') = exp_schema.dataset_expression_name
            '''

            merged = self.spark_session.sql(sql)
            self.BigQuery_table_utilities.write_to_BigQuery(df=merged, output_path='Tables/pbi_dataset_datasources_parsed')
            logger.info('Data source parameters updated and written to table successfully.')

            return True

        except Exception as e:
            message = f'Error updating data source parameters: {e}'
            logger.error(message)
            raise

