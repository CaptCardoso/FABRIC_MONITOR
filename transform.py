import pytz
import pathlib
import shutil
import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode_outer, explode
from shared_code.utils.helpers import setup_logging

try:
    import notebookutils.mssparkutils as utils
except ImportError:
    import pyspark.dbutils as utils


# Create logger
logger = logging.getLogger('TransformationService')

class TransformationService:
    """
    Contains functions for transforming activity and catalog JSON data and writing it to delta.

    Attributes:
        spark_session: The SparkSession object.
        fabric_monitor_service: An instance of the FabricMonitorService class.
        fabric_monitor_schema: An instance of the FabricMonitorSchema class.
        BigQuery_table_utilities: An instance of the BigQueryUtilities class.
    """

    def __init__(self, spark_session, fabric_monitor_service, fabric_monitor_schema, delta_table_utilities, storage_client):
        self.spark_session = spark_session
        self.fabric_monitor_service = fabric_monitor_service
        self.fabric_monitor_schema = fabric_monitor_schema
        self.delta_table_utilities = delta_table_utilities
        self.storage_client = storage_client
        self.utc_timestamp = datetime.now(pytz.utc)


    def transform_activity_data(self, bucket_name='fabric-monitor-data'):
        """
        Reads all unprocessed (i.e. incremental) activity data extracts from the lakehouse using Spark, extracts and renames the desired fields
        based on a pre-defined schema, writes the data to corresponding delta table, and moves the source data from the "unprocessed"
        to "processed" folders.

        Returns:
            None
        """

        try:
            unprocessed_activity_path_relative = 'Files/Activities/unprocessed'
            unprocessed_activity_path = f'gs://{bucket_name}/{unprocessed_activity_path_relative}'
            unprocessed_activity_extracts = [blob.name for blob in self.storage_client.list_blobs(bucket_name, prefix='Files/Activities/unprocessed')]
            message = f'There are {len(unprocessed_activity_extracts)} unprocessed activity extract(s)...'
            message = f'There are {len(unprocessed_activity_extracts)} unprocessed activity extract(s)...'
            logger.info(message)

            if not unprocessed_activity_extracts:
                message = 'There are no activity scans to process. Skipping activity data processing.'
                logger.info(message)
            else:
                schema = self.fabric_monitor_schema.usage_schema
                activities = self.spark_session.read.schema(schema).json(f'{unprocessed_activity_path}/*/*/*/*.json')
                activity_df = self.fabric_monitor_service.create_activity_values(activities)

                artifacts = {
                    'pbi_activity': {"df": activity_df, "merge_keys": ['activity_id'], "merge_type": "delete_insert"}
                }

                try:
                    logger.info('Starting activity data merge...')
                    for table, parameters in artifacts.items():
                        self.delta_table_utilities.update_bigquery_table(table, parameters)

                except Exception as e:
                    message = f'Error merging activity data: {e}'
                    logger.error(message)

                # Move to processed folder
                for extract in unprocessed_activity_extracts:
                    message = f'Moving extract {extract} from unprocessed to processed...'
                    logger.info(message)

                    bucket = self.storage_client.bucket(bucket_name)
                    current_path = str(extract)
                    blob = bucket.blob(current_path)
                    new_path = current_path.replace('unprocessed', 'processed')

                    copy = bucket.copy_blob(blob, bucket, new_path)
                    blob.delete()

                message = 'Activity data processing complete.'
                logger.info(message)

        except Exception as e:
            message = f'Error in transform_activity_data method: {e}'
            logger.error(message)


    def transform_catalog_data(self):
        """
        Reads all unprocessed (i.e. incremental) catalog extracts from the lakehouse using Spark, extracts and renames the desired fields
        based on a pre-defined schema, writes the data to corresponding delta table, and moves the source data from the "unprocessed"
        to "processed" folders.

        Returns:
            None
        """
        try:
            unprocessed_scans_path_relative = 'Files/Catalog/unprocessed/scans'
            unprocessed_scans_path_file_api = f'/lakehouse/default/{unprocessed_scans_path_relative}'
            unprocessed_scans = pathlib.Path(unprocessed_scans_path_file_api)
            unprocessed_scan_extracts = list(unprocessed_scans.rglob('*.json'))

            message = f'There are {len(unprocessed_scan_extracts)} unprocessed scan(s)...'
            logger.info(message)

            if not unprocessed_scan_extracts:
                message = 'There are no catalog scans to process. Skipping catalog data processing.'
                logger.info(message)
            else:
                try:
                    schema = self.fabric_monitor_schema.catalog_schema
                    df = self.spark_session.read.schema(schema).json(f'{unprocessed_scans_path_relative}/*/*/*/*.json')
                    message = 'Catalog JSON successfully read.'
                    logger.info(message)

                except Exception as e:
                    message = f'Error reading catalog JSON: {e}'
                    logger.error(message)
                    utils.notebook.exit('Error reading catalog JSON. Notebook execution halted.')

                try:
                    workspaces = df.select(explode("workspaces").alias("workspace"))
                    reports = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.reports')).alias("report"))
                    dashboards = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.dashboards')).alias("dashboard"))
                    dashboard_tiles = dashboards.select(col('workspace_id'), col('dashboard.id').alias('dashboard_id'), explode(col('dashboard.tiles')).alias("tile"))
                    datasets = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.datasets')).alias("dataset"))
                    dataset_datasource_usages = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.datasourceUsages')).alias("datasource_usage"))
                    dataset_expressions = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.expressions')).alias("expressions"))
                    upstream_datasets = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.upstreamDatasets')).alias("upstream_datasets"))
                    upstream_dataflows = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.upstreamDataflows')).alias("upstream_dataflows"))
                    dataset_tables = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.tables')).alias("table"))
                    dataset_table_columns = dataset_tables.select(col('workspace_id'), col('dataset_id'), col('table.name').alias('table_name'), explode(col('table.columns')).alias("column"))
                    dataset_table_measures = dataset_tables.select(col('workspace_id'), col('dataset_id'), col('table.name').alias('table_name'), explode(col('table.measures')).alias("measure"))
                    dataset_table_sources = dataset_tables.select(col('workspace_id'), col('dataset_id'), col('table.name').alias('table_name'), explode(col('table.source')).alias("source"))
                    dataset_table_sources = dataset_tables.select(col('workspace_id'), col('dataset_id'), col('table.name').alias('table_name'), explode(col('table.source')).alias("source"))
                    dataflows = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.dataflows')).alias("dataflow"))
                    datamarts = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.datamarts')).alias("datamart"))
                    datasources = df.select(explode("datasourceInstances").alias("datasource")).distinct() # Appears to have repetition across workspaces

                    ### Fabric items
                    lakehouses = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.Lakehouse')).alias("lakehouse"))
                    datapipelines = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.DataPipeline')).alias("datapipeline"))
                    ###

                    workspace_user_permissions = workspaces.select(col('workspace.id').alias('workspace_id'), explode(col('workspace.users')).alias("user"))
                    report_user_permissions = reports.select(col('workspace_id'), col('report.id').alias('report_id'), explode(col('report.users')).alias("user"))
                    dashboard_user_permissions = dashboards.select(col('workspace_id'), col('dashboard.id').alias('dashboard_id'), explode(col('dashboard.users')).alias("user"))
                    dataset_user_permissions = datasets.select(col('workspace_id'), col('dataset.id').alias('dataset_id'), explode(col('dataset.users')).alias("user"))
                    dataflow_user_permissions = dataflows.select(col('workspace_id'), col('dataflow.objectId').alias('dataflow_id'), explode(col('dataflow.users')).alias("user"))
                    lakehouse_user_permissions = lakehouses.select(col('workspace_id'), col('lakehouse.id').alias('lakehouse_id'), explode(col('lakehouse.users')).alias("user"))
                    datapipeline_user_permissions = datapipelines.select(col('workspace_id'), col('datapipeline.id').alias('datapipeline_id'), explode(col('datapipeline.users')).alias("user"))

                    user_dfs = {
                        "workspace": workspace_user_permissions,
                        "report": report_user_permissions,
                        "dashboard": dashboard_user_permissions,
                        "dataset": dataset_user_permissions,
                        "dataflow": dataflow_user_permissions,
                        "lakehouse": lakehouse_user_permissions,
                        "datapipeline": datapipeline_user_permissions
                    }

                    transformed_user_permissions_dfs = {
                        k: self.fabric_monitor_service.create_artifact_users_values(df=v, artifact_type=k) for k, v in user_dfs.items()
                    }

                    users_permissions_df = (
                        transformed_user_permissions_dfs['workspace']
                        .union(transformed_user_permissions_dfs['report'])
                        .union(transformed_user_permissions_dfs['dashboard'])
                        .union(transformed_user_permissions_dfs['dataset'])
                        .union(transformed_user_permissions_dfs['dataflow'])
                        .union(transformed_user_permissions_dfs['lakehouse'])
                        .union(transformed_user_permissions_dfs['datapipeline'])
                    )

                    # Read tenant data from JSON
                    tenant_data_base_path = 'Files/Tenant Data' # Need relative file path since using Spark
                    refreshables = self.spark_session.read.json(f'{tenant_data_base_path}/refreshables.json')
                    capacities = self.spark_session.read.json(f'{tenant_data_base_path}/capacities.json')
                    apps = self.spark_session.read.json(f'{tenant_data_base_path}/apps.json')
                    tenant_settings = self.spark_session.read.json(f'{tenant_data_base_path}/tenant_settings.json')

                    # Rename all columns using pre-defined functions
                    workspaces_df = self.fabric_monitor_service.create_workspace_values(workspaces)
                    reports_df = self.fabric_monitor_service.create_report_values(reports)
                    datasets_df = self.fabric_monitor_service.create_dataset_values(datasets)
                    dataset_datasources_df = self.fabric_monitor_service.create_dataset_datasource_values(dataset_datasource_usages)
                    dataset_expressions_df = self.fabric_monitor_service.create_dataset_expression_values(dataset_expressions)
                    upstream_datasets_df = self.fabric_monitor_service.create_dataset_upstream_datasets_values(upstream_datasets)
                    upstream_dataflows_df = self.fabric_monitor_service.create_dataset_upstream_dataflow_values(upstream_dataflows)
                    tables_df = self.fabric_monitor_service.create_table_values(dataset_tables)
                    columns_df = self.fabric_monitor_service.create_column_values(dataset_table_columns)
                    measures_df = self.fabric_monitor_service.create_measure_values(dataset_table_measures)
                    table_sources_df = self.fabric_monitor_service.create_table_sources_values(dataset_table_sources)
                    datasources_df = self.fabric_monitor_service.create_datasource_values(datasources)
                    dashboards_df = self.fabric_monitor_service.create_dashboard_values(dashboards)
                    dashboard_tiles_df = self.fabric_monitor_service.create_dashboard_tile_values(dashboard_tiles)
                    dataflows_df = self.fabric_monitor_service.create_dataflow_values(dataflows)
                    datamarts_df = self.fabric_monitor_service.create_datamart_values(datamarts)

                    ### Fabric items
                    lakehouses_df = self.fabric_monitor_service.create_lakehouse_values(lakehouses)
                    lakehouse_relations_df = self.fabric_monitor_service.create_lakehouse_relation_values(lakehouses)
                    datapipelines_df = self.fabric_monitor_service.create_datapipeline_values(datapipelines)
                    datapipeline_relations_df = self.fabric_monitor_service.create_datapipeline_relation_values(datapipelines)
                    ###

                    refreshables_df = self.fabric_monitor_service.create_refreshable_values(refreshables)
                    capacities_df = self.fabric_monitor_service.create_capacity_values(capacities)
                    apps_df = self.fabric_monitor_service.create_app_values(apps)
                    tenant_settings_df = self.fabric_monitor_service.create_tenant_settings_values(tenant_settings)
                    tenant_settings_properties_df = self.fabric_monitor_service.create_tenant_settings_properties_values(tenant_settings)
                    tenant_setting_groups_df = self.fabric_monitor_service.create_tenant_settings_security_groups_values(tenant_settings)

                    message = 'Catalog object dataframes successfully created.'
                    logger.info(message)

                except Exception as e:
                    message = f'Error creating catalog object dataframes: {e}'
                    logger.error(message)
                    utils.notebook.exit('Error creating catalog dataframes. Notebook execution halted.')

                artifacts = {
                    'pbi_dataset_expressions': {"df": dataset_expressions_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_upstream_datasets': {"df": upstream_datasets_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_upstream_dataflows': {"df": upstream_dataflows_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_workspaces': {"df": workspaces_df, "merge_keys": ['workspace_id'], "merge_type": "delete_insert"},
                    'pbi_reports': {"df": reports_df, "merge_keys": ['report_id'], "merge_type": "delete_insert"},
                    'pbi_datasets': {"df": datasets_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_dashboards': {"df": dashboards_df, "merge_keys": ['dashboard_id'], "merge_type": "delete_insert"},
                    'pbi_dashboard_tiles': {"df": dashboard_tiles_df, "merge_keys": ['dashboard_id'], "merge_type": "delete_insert"},
                    'pbi_dataflows': {"df": dataflows_df, "merge_keys": ['dataflow_id'], "merge_type": "delete_insert"},
                    'pbi_datasources': {"df": datasources_df, "merge_keys": ['datasource_id'], "merge_type": "delete_insert"},
                    'pbi_capacities': {"df": capacities_df, "merge_keys": ['capacity_id'], "merge_type": "delete_insert"},
                    'pbi_refreshables': {"df": refreshables_df, "merge_keys": ['refreshable_id'], "merge_type": "delete_insert"},
                    'pbi_tables': {"df": tables_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_columns': {"df": columns_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_measures': {"df": measures_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_table_sources': {"df": table_sources_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_datamarts': {"df": datamarts_df, "merge_keys": ['datamart_id'], "merge_type": "delete_insert"},
                    'pbi_apps': {"df": apps_df, "merge_keys": ['app_id'], "merge_type": "delete_insert"},
                    'pbi_user_permissions': {"df": users_permissions_df, "merge_keys": ['artifact_id'], "merge_type": "delete_insert"},
                    'pbi_tenant_settings': {"df": tenant_settings_df, "merge_keys": ['settingName'], "merge_type": "delete_insert"},
                    'pbi_tenant_setting_properties': {"df": tenant_settings_properties_df, "merge_keys": ['settingName'], "merge_type": "delete_insert"},
                    'pbi_tenant_setting_groups': {"df": tenant_setting_groups_df, "merge_keys": ['settingName'], "merge_type": "delete_insert"},
                    'pbi_dataset_datasources': {"df": dataset_datasources_df, "merge_keys": ['dataset_id'], "merge_type": "delete_insert"},
                    'pbi_lakehouses': {"df": lakehouses_df, "merge_keys": ['lakehouse_id'], "merge_type": "delete_insert"},
                    'pbi_lakehouse_relations': {"df": lakehouse_relations_df, "merge_keys": ['lakehouse_id'], "merge_type": "delete_insert"},
                    'pbi_datapipelines': {"df": datapipelines_df, "merge_keys": ['datapipeline_id'], "merge_type": "delete_insert"},
                    'pbi_datapipeline_relations': {"df": datapipeline_relations_df, "merge_keys": ['datapipeline_id'], "merge_type": "delete_insert"}
                }

                try:
                    logger.info('Starting catalog merges...')
                    for table, parameters in artifacts.items():
                        self.delta_table_utilities.write_and_optimize_delta_table(table, parameters)

                except Exception as e:
                    message = f'Error merging catalog data: {e}'
                    logger.error(message)
                    utils.notebook.exit('Error merging catalog data. Notebook execution halted.')

                # Move scans to processed folder
                try:
                    for scan in unprocessed_scan_extracts:
                        message = f'Moving scan {scan} from unprocessed to processed...'
                        logger.info(message)
                        current_path = str(scan)
                        new_path = current_path.replace('unprocessed', 'processed')
                        shutil.move(current_path, new_path)

                    message = 'Catalog data processing complete.'
                    logger.info(message)

                except Exception as e:
                    message = f'Error moving scans to processed folder: {e}'
                    logger.error(message)
                    utils.notebook.exit('Error moving catalog scans to processed folder. Notebook execution halted.')

        except Exception as e:
            logger.error(f'Error in transform_catalog_data method: {e}')
            utils.notebook.exit('Error in transform_catalog_data method. Notebook execution halted.')


    def transform_graph_api_data(self):
        """
        Reads all unprocessed (i.e. incremental) Graph API extracts from the lakehouse using Spark, extracts and renames the desired fields
        based on a pre-defined schema, writes the data to corresponding delta table.

        Returns:
            None
        """
        try:
            # Read user data from Graph API
            graph_data_base_path = 'Files/Graph Data' # Need relative file path since using Spark
            users = self.spark_session.read.json(f'{graph_data_base_path}/users.json')
            user_licenses = users.select(col('id'), explode(col('assignedLicenses')).alias('assignedLicenses'))
            user_licenses = user_licenses.select(col('id'), col('assignedLicenses').skuId.alias('skuId'), explode_outer(col('assignedLicenses').disabledPlans).alias('disabled_plans'))
            subscribed_skus = self.spark_session.read.json(f'{graph_data_base_path}/subscribed_skus.json')

            subscribed_skus_df = self.fabric_monitor_service.create_subscribed_sku_values(subscribed_skus)
            users_df = self.fabric_monitor_service.create_user_values(users)
            user_licenses_df = self.fabric_monitor_service.create_user_license_values(user_licenses)

            message = 'Graph API object dataframes successfully created.'
            logger.info(message)

        except Exception as e:
            message = f'Error creating Graph API object dataframes: {e}'
            logger.error(message)
            utils.notebook.exit('Error creating Graph API object dataframes. Notebook execution halted.')

        artifacts = {
            'pbi_subscribed_skus': {"df": subscribed_skus_df, "merge_keys": ['subscribed_sku_id'], "merge_type": "delete_insert"},
            'pbi_users': {"df": users_df, "merge_keys": ['user_id'], "merge_type": "delete_insert"},
            'pbi_user_licenses': {"df": user_licenses_df, "merge_keys": ['user_id'], "merge_type": "delete_insert"}
        }

        try:
            logger.info('Starting Graph API merges...')
            for table, parameters in artifacts.items():
                self.delta_table_utilities.write_and_optimize_delta_table(table, parameters)

        except Exception as e:
            message = f'Error merging Graph API data: {e}'
            logger.error(message)
            utils.notebook.exit('Error merging Graph API data. Notebook execution halted.')