from pyspark.sql import SparkSession
# Import required services from Fabric Monitor package
from shared_code.authentication_service import AuthService
from shared_code.fabric_service import FabricService
from shared_code.graph_service import GraphService
from shared_code.fabric_monitor_service import FabricMonitorService
from shared_code.fabric_monitor_db import FabricMonitorDb
from shared_code.fabric_monitor_schema import FabricMonitorSchema
from shared_code.utils.scan_status import ScanStatusUtilities
from shared_code.utils.bigquery_table import BigQueryUtilities
from extract import ExtractionService
from transform import TransformationService
from parse import ParsingService

# Create a Spark session
spark = SparkSession.builder.appName("Fabric Monitor").getOrCreate()

# Define authentication parameters
tenant_id = {tenant_id}
client_id = {client_id}
client_secret = {client_secret}


### Instantiate helper services
fabric_service_access_token = AuthService(tenant_id, client_id, client_secret, 'https://analysis.windows.net/powerbi/api').get_auth_token()
fabric_service = FabricService('myorg', fabric_service_access_token)

graph_service_access_token = AuthService(tenant_id, client_id, client_secret, 'https://graph.microsoft.com').get_auth_token()
graph_service = GraphService(graph_service_access_token)
fabric_monitor_service = FabricMonitorService(spark)
fabric_monitor_db = FabricMonitorDb(spark)
fabric_monitor_schema = FabricMonitorSchema()
scan_status_utilities = ScanStatusUtilities(spark)
BigQuery_table_utilities = BigQueryUtilities(spark)

# ### Extract Catalog and Activity Data

# Instantiate ExtractionService
extraction_service = ExtractionService(fabric_service, graph_service, fabric_monitor_service, fabric_monitor_db, scan_status_utilities)

# Extract Activity Data
extract_activity_data = extraction_service.extract_activity_data(output_base_path='Files/Activities/unprocessed')

# Extract Catalog data
extract_catalog_data = extraction_service.extract_catalog_data(output_base_path='Files/Catalog/unprocessed', full_scan=False)

# Extract app, capacity, refreshable, and tenant_settings data
extract_power_bi_admin_api_data = extraction_service.extract_power_bi_admin_api_data()

# Extract user and sku data
extract_graph_api_data = extraction_service.extract_graph_api_data()


# # Transform Activity Data

# transformation_service = TransformationService(spark, fabric_monitor_service, fabric_monitor_schema, BigQuery_table_utilities)

# transformation_service.transform_activity_data()

# # Transform Catalog Data

# transformation_service.transform_catalog_data()


# # Transform Graph API Data

# transformation_service.transform_graph_api_data()


# # Parse Connection Source Information

# # Instantiate ExtractionService
# parsing_service = ParsingService(spark_session=spark, BigQuery_table_utilities=BigQuery_table_utilities)

# # Parse connection information
# parsing_service.parse_connection_information()

# # Parse dataset expressions
# parsing_service.parse_dataset_expressions()

# # Update data source parameters and write to table
# parsing_service.update_data_source_parameters_and_write_to_table()

# # Refresh datasets in Power BI

# # Refresh Power BI data models

# datasets = ['Enterprise Data Dictionary', 'Tenant Settings', 'User Activity and Tenant Catalog']

# for dataset in datasets:
#     fabric_service.refresh_dataset(dataset)


spark.stop()