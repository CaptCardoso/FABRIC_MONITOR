import logging
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import col, concat_ws, lit, initcap, from_json, explode
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('FabricMonitorService')

class FabricMonitorService:
    """
    A class that provides methods to manipulate and write data extracted from the Fabric/Power BI APIs.
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session


    def create_workspace_values(self, df):

        return df.select(
            col("workspace.id").alias("workspace_id"),
            col("workspace.name").alias("workspace_name"),
            col("workspace.type").alias("workspace_type"),
            col("workspace.state").alias("workspace_state"),
            col("workspace.isOnDedicatedCapacity"),
            col("workspace.capacityId"),
            col("workspace.defaultDatasetStorageFormat"),
            col("workspace.dataRetrievalState"),
            col("workspace.description")
        )


    def create_report_values(self, df):

        return df.select(
            col('workspace_id'),
            col('report.id').alias('report_id'),
            col('report.name').alias('report_name'),
            col('report.datasetId').alias('report_datasetId'),
            col('report.datasetWorkspaceId').alias('report_datasetWorkspaceId'),
            col('report.createdDateTime').alias('report_createdDateTime'),
            col('report.modifiedDateTime').alias('report_modifiedDateTime'),
            col('report.endorsementDetails.endorsement').alias('report_endorsement'),
            col('report.endorsementDetails.certifiedBy').alias('report_certifiedBy'),
            col('report.sensitivityLabel.labelId').alias('report_sensitivity_labelId'),
            col('report.appId').alias('report_appId'),
            col('report.createdById').alias('report_createdById'),
            col('report.createdBy').alias('report_createdBy'),
            col('report.modifiedById').alias('report_modifiedById'),
            col('report.modifiedBy').alias('report_modifiedBy'),
            col('report.description').alias('report_description'),
            col('report.originalReportObjectId').alias('report_originalReportObjectId'),
            col('report.reportType').alias('report_reportType')
        )


    def create_dataset_values(self, df):

        return df.select(
            col('workspace_id'),
            col('dataset.id').alias('dataset_id'),
            col('dataset.name').alias('dataset_name'),
            col('dataset.targetStorageMode').alias('dataset_targetStorageMode'),
            col('dataset.endorsementDetails.endorsement').alias('dataset_endorsement'),
            col('dataset.endorsementDetails.certifiedBy').alias('dataset_certifiedBy'),
            col('dataset.sensitivityLabel.labelId').alias('dataset_sensitivity_labelId'),
            col('dataset.configuredBy').alias('dataset_configuredBy'),
            col('dataset.configuredById').alias('dataset_configuredById'),
            col('dataset.contentProviderType').alias('dataset_contentProviderType'),
            col('dataset.createdDate').alias('dataset_createdDate'),
            col('dataset.description').alias('dataset_description'),
            col('dataset.isEffectiveIdentityRequired').alias('dataset_isEffectiveIdentityRequired'),
            col('dataset.isEffectiveIdentityRolesRequired').alias('dataset_isEffectiveIdentityRolesRequired'),
            col('dataset.schemaMayNotBeUpToDate').alias('dataset_schemaMayNotBeUpToDate'),
            col('dataset.schemaRetrievalError').alias('dataset_schemaRetrievalError')
        )


    def create_dashboard_values(self, df):

        return df.select(
            col('workspace_id'),
            col('dashboard.id').alias('dashboard_id'),
            col('dashboard.displayName').alias('dashboard_displayName'),
            col('dashboard.appId').alias('dashboard_appId'),
            col('dashboard.isReadOnly').alias('dashboard_isReadOnly')
        )


    def create_dataflow_values(self, df):

        return df.select(
            col('workspace_id'),
            col('dataflow.objectId').alias('dataflow_id'),
            col('dataflow.name').alias('dataflow_name'),
            col('dataflow.description').alias('dataflow_description'),
            col('dataflow.configuredBy').alias('dataflow_configuredBy'),
            col('dataflow.endorsementDetails.endorsement').alias('dataflow_endorsement'),
            col('dataflow.modifiedBy').alias('dataflow_modifiedBy'),
            col('dataflow.modifiedDateTime').alias('dataflow_modifiedDateTime'),
            col('dataflow.sensitivityLabel.labelId').alias('dataflow_sensitivity_labelId')
        )


    def create_datasource_values(self, df):

        return df.select(
            col('datasource.datasourceId').alias('datasource_id'),
            col('datasource.datasourceType').alias('datasourceType'),
            col('datasource.gatewayId').alias('gatewayId'),
            col('datasource.connectionDetails.extensionDataSourceKind').alias('extensionDataSourceKind'),
            col('datasource.connectionDetails.extensionDataSourcePath').alias('extensionDataSourcePath'),
            col('datasource.connectionDetails.path').alias('path'),
            col('datasource.connectionDetails.sharePointSiteUrl').alias('sharePointSiteUrl'),
            col('datasource.connectionDetails.url').alias('url'),
            col('datasource.connectionDetails.server').alias('server'),
            col('datasource.connectionDetails.database').alias('database'),
            col('datasource.connectionDetails.account').alias('account'),
            col('datasource.connectionDetails.classInfo').alias('classInfo'),
            col('datasource.connectionDetails.connectionString').alias('connectionString'),
            col('datasource.connectionDetails.domain').alias('domain'),
            col('datasource.connectionDetails.emailAddress').alias('emailAddress'),
            col('datasource.connectionDetails.loginServer').alias('loginServer')
        )


    def create_capacity_values(self, df):

        return df.select(
            col('id').alias('capacity_id'),
            col('displayName').alias('displayName'),
            col('admins').alias('admins'),
            col('sku').alias('sku'),
            col('state').alias('capacity_state'),
            col('capacityUserAccessRight').alias('capacityUserAccessRight'),
            col('region').alias('region'),
            col('users').alias('users')
        )


    def create_refreshable_values(self, df):

        return df.select(
            col('id').alias('refreshable_id'),
            col('name').alias('name'),
            col('kind').alias('kind'),
            col('startTime').alias('startTime'),
            col('endTime').alias('endTime'),
            col('refreshesPerDay').alias('refreshesPerDay'),
            col('refreshCount').alias('refreshCount'),
            col('refreshFailures').alias('refreshFailures'),
            col('averageDuration').alias('averageDuration'),
            col('medianDuration').alias('medianDuration'),
            col('configuredBy').alias('configuredBy'),
            col('lastRefresh').alias('lastRefresh'),
            col('refreshSchedule').alias('refreshSchedule')
        )


    def create_app_values(self, df):

        return df.select(
            col('id').alias('app_id'),
            col('name').alias('name'),
            col('description').alias('description'),
            col('publishedBy').alias('publishedBy'),
            col('lastUpdate').alias('lastUpdate')
        )


    def create_table_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('table.name')).alias('table_id'),
            col('dataset_id'),
            col('table.name').alias('table_name'),
            col('table.isHidden').alias('table_isHidden'),
            col('table.description').alias('table_description')
        )


    def create_dataset_datasource_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('datasource_usage.datasourceInstanceId')).alias('dataset_datasource_id'),
            col('dataset_id'),
            col('datasource_usage.datasourceInstanceId').alias('datasource_id')
        )


    def create_dataset_expression_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('expressions.name')).alias('dataset_expression_id'),
            col('dataset_id'),
            col('expressions.name').alias('dataset_expression_name'),
            col('expressions.expression').alias('dataset_expression')
        )


    def create_dataset_upstream_datasets_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('upstream_datasets.targetDatasetId')).alias('dataset_upstream_dataset_id'),
            col('dataset_id'),
            col('upstream_datasets.targetDatasetId').alias('upstream_dataset_id'),
            col('upstream_datasets.groupId').alias('upstream_dataset_workspace_id')
        )


    def create_dataset_upstream_dataflow_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('upstream_dataflows.targetDataflowId')).alias('dataset_upstream_dataflow_id'),
            col('dataset_id'),
            col('upstream_dataflows.targetDataflowId').alias('upstream_dataflow_id'),
            col('upstream_dataflows.groupId').alias('upstream_dataflow_workspace_id')
        )


    def create_column_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('table_name'), col('column.name')).alias('column_id'),
            col('dataset_id'),
            col('table_name'),
            col('column.name').alias('column_name'),
            col('column.columnType').alias('column_columnType'),
            col('column.dataType').alias('column_dataType'),
            col('column.isHidden').alias('column_isHidden'),
            col('column.description').alias('column_description'),
            col('column.expression').alias('column_expression')
        )


    def create_measure_values(self, df):

        return df.select(
            concat_ws('-', col('dataset_id'), col('table_name'), col('measure.name')).alias('measure_id'),
            col('dataset_id'),
            col('table_name'),
            col('measure.name').alias('measure_name'),
            col('measure.expression').alias('measure_expression'),
            col('measure.isHidden').alias('measure_isHidden'),
            col('measure.description').alias('measure_description')
        )


    def create_table_sources_values(self, df):

        return df.select(
            concat_ws('-', col('workspace_id'), col('dataset_id'), col('table_name')).alias('table_id'),
            col('workspace_id'),
            col('dataset_id'),
            col('table_name'),
            col('source.expression').alias('source_expression')
        )


    def create_dashboard_tile_values(self, df):
        return df.select(
            col('workspace_id'),
            col('dashboard_id'),
            col('tile.id').alias('dashboard_tile_id'),
            col('tile.datasetWorkspaceId').alias('dashboard_tile_datasetWorkspaceId'),
            col('tile.datasetId').alias('dashboard_tile_datasetId'),
            col('tile.reportId').alias('dashboard_tile_reportId'),
            col('tile.title').alias('dashboard_tile_title'),
            col('tile.subTitle').alias('dashboard_tile_subTitle')
        )


    def create_artifact_users_values(self, df, artifact_type):

        if artifact_type in ['report', 'dashboard', 'dataset', 'dataflow', 'datamart']:
            user_access_right = f'{artifact_type}UserAccessRight'
        elif artifact_type == 'workspace':
            user_access_right = 'groupUserAccessRight'
        else:
            user_access_right = 'artifactUserAccessRight'

        return df.select(
            col('workspace_id'),
            col(f'{artifact_type}_id').alias('artifact_id'),
            initcap(lit(f'{artifact_type}')).alias('artifact_type'),
            col('user.displayName').alias('user_displayName'),
            col('user.emailAddress').alias('user_emailAddress'),
            col(f'user.{user_access_right}').alias('access_right'),
            col('user.identifier').alias('user_identifier'),
            col('user.graphId').alias('user_graphId'),
            col('user.principalType').alias('user_principalType'),
            col('user.userType').alias('user_userType')
        )


    def create_datamart_values(self, df):

        return df.select(
            col('workspace_id'),
            col('datamart.id').alias('datamart_id'),
            col('datamart.name').alias('datamart_name'),
            col('datamart.description').alias('datamart_description'),
            col('datamart.configuredBy').alias('datamart_configuredBy'),
            col('datamart.configuredById').alias('datamart_configuredById'),
            #col('datamart.endorsementDetails.endorsement').alias('datamart_endorsement'),
            col('datamart.modifiedBy').alias('datamart_modifiedBy'),
            col('datamart.modifiedById').alias('datamart_modifiedById'),
            col('datamart.modifiedDateTime').alias('datamart_modifiedDateTime'),
            col('datamart.type').alias('datamart_type')
        )


    def create_tenant_settings_values(self, df):

        return df.select(
            col('settingName'),
            col('tenantSettingGroup'),
            col('title'),
            col('enabled'),
            col('canSpecifySecurityGroups')
        )


    def create_tenant_settings_properties_values(self, df):

        return df.selectExpr('settingName','inline(properties)')


    def create_tenant_settings_security_groups_values(self, df):

        return df.selectExpr('settingName','inline(enabledSecurityGroups)')


    def create_user_values(self, df):

       return df.select(
            col('id').alias('user_id'),
            col('displayName').alias('user_display_name'),
            col('UserPrincipalName').alias('user_principal_name')
        )


    def create_user_license_values(self, df):

       return df.select(
            col('id').alias('user_id'),
            col('skuId').alias('subscribed_sku_id'),
            col('disabled_plans').alias('disabled_plans')
        )


    def create_subscribed_sku_values(self, df):

       return df.select(
            col('id').alias('subscribed_sku_id'),
            col('skuPartNumber').alias('subscribed_sku_name'),
            col('capabilityStatus').alias('subscribed_sku_status'),
            col('skuId').alias('subscribed_sku_skuId')
        )


# Fabric artifacts
    def create_lakehouse_values(self, df):

        dw_properties_schema = StructType([
            StructField("tdsEndpoint", StringType(), True),
            StructField("id", StringType(), True),
            StructField("provisioningStatus", StringType(), True)
        ])

        df = df.withColumn("DwPropertiesStruct", from_json("lakehouse.extendedProperties.DwProperties", dw_properties_schema))

        return df.select(
            col('workspace_id'),
            col('lakehouse.id').alias('lakehouse_id'),
            col('lakehouse.createdBy').alias('lakehouse_createdBy'),
            col('lakehouse.createdById').alias('lakehouse_createdById'),
            col('lakehouse.description').alias('lakehouse_description'),
            col('DwPropertiesStruct.tdsEndpoint').alias('lakehouse_tdsEndpoint'),
            col('DwPropertiesStruct.id').alias('lakehouse_tdsEndpointId'),
            col('DwPropertiesStruct.provisioningStatus').alias('lakehouse_provisioningStatus'),
            col('lakehouse.extendedProperties.OneLakeFilesPath').alias('lakehouse_OneLakeFilesPath'),
            col('lakehouse.extendedProperties.OneLakeTablesPath').alias('lakehouse_OneLakeTablesPath'),
            col('lakehouse.lastUpdatedDate').alias('lakehouse_lastUpdatedDate'),
            col('lakehouse.modifiedBy').alias('lakehouse_modifiedBy'),
            col('lakehouse.modifiedById').alias('lakehouse_modifiedById'),
            col('lakehouse.name').alias('lakehouse_name'),
            col('lakehouse.sensitivityLabel.labelId').alias('lakehouse_sensitivity_labelId'),
            col('lakehouse.state').alias('lakehouse_state')
        )


    def create_lakehouse_relation_values(self, df):

        return df.select(
            col('lakehouse.id').alias('lakehouse_id'),
            explode('lakehouse.relations').alias('relation')
        ).select(
            'lakehouse_id',
            col('relation.dependentOnArtifactId').alias('relation_dependentOnArtifactId'),
            col('relation.relationType').alias('relation_relationType'),
            col('relation.settingsList').alias('relation_settingsList'),
            col('relation.usage').alias('relation_usage'),
            col('relation.workspaceId').alias('relation_workspaceId')
        )


    def create_datapipeline_values(self, df):

        return df.select(
            col('workspace_id'),
            col('datapipeline.id').alias('datapipeline_id'),
            col('datapipeline.createdBy').alias('lakehouse_createdBy'),
            col('datapipeline.createdById').alias('datapipeline_createdById'),
            col('datapipeline.description').alias('datapipeline_description'),
            col('datapipeline.endorsementDetails.certifiedBy').alias('datapipeline_certifiedBy'),
            col('datapipeline.endorsementDetails.endorsement').alias('datapipeline_endorsement'),
            col('datapipeline.lastUpdatedDate').alias('datapipeline_lastUpdatedDate'),
            col('datapipeline.modifiedBy').alias('datapipeline_modifiedBy'),
            col('datapipeline.modifiedById').alias('datapipeline_modifiedById'),
            col('datapipeline.name').alias('datapipeline_name'),
            col('datapipeline.sensitivityLabel.labelId').alias('datapipeline_sensitivity_labelId'),
            col('datapipeline.state').alias('datapipeline_state')
        )


    def create_datapipeline_relation_values(self, df):

        return df.select(
            col('datapipeline.id').alias('datapipeline_id'),
            explode('datapipeline.relations').alias('relation')
        ).select(
            'datapipeline_id',
            col('relation.dependentOnArtifactId').alias('relation_dependentOnArtifactId'),
            col('relation.relationType').alias('relation_relationType'),
            col('relation.settingsList').alias('relation_settingsList'),
            col('relation.usage').alias('relation_usage'),
            col('relation.workspaceId').alias('relation_workspaceId')
        )


# Activity data
    def create_activity_values(self, df):
        return df.select(
            col('Id').alias('activity_id'),
            col('ActivityId'),
            col('Activity'),
            col('Operation'),
            col('ActionSource'),
            col('ActionSourceDetail'),
            col('AppId'),
            col('AppName'),
            col('AppReportId'),
            col('ArtifactId'),
            col('ArtifactKind'),
            col('ArtifactName'),
            col('ArtifactType'),
            col('CapacityId'),
            col('CapacityName'),
            col('CapacityState'),
            col('CapacityUsers'),
            col('ClientIP'),
            col('ConsumptionMethod'),
            col('CopiedReportId'),
            col('CopiedReportName'),
            col('CreationTime'),
            col('CustomVisualAccessTokenResourceId'),
            col('CustomVisualAccessTokenSiteUri'),
            col('DashboardId'),
            col('DashboardName'),
            col('DataClassification'),
            col('DataConnectivityMode'),
            col('DataflowAllowNativeQueries'),
            col('DataflowId'),
            col('DataflowName'),
            col('DataflowRefreshScheduleType'),
            col('DataflowType'),
            col('DatasetId'),
            col('DatasetName'),
            col('DatasourceDetails'),
            col('DatasourceId'),
            col('DeploymentPipelineId'),
            col('DeploymentPipelineObjectId'),
            col('DistributionMethod'),
            col('EmbedTokenId'),
            col('EndPoint'),
            col('ExcludePersonalWorkspaces'),
            col('Experience'),
            col('ExportEventEndDateTimeParameter'),
            col('ExportEventStartDateTimeParameter'),
            col('ExportedArtifactInfo.ArtifactId').alias('ExportedArtifactInfoArtifactId'),
            col('ExportedArtifactInfo.ArtifactType').alias('ExportedArtifactInfoArtifactType'),
            col('ExportedArtifactInfo.ExportType').alias('ExportedArtifactInfoExportType'),
            col('FolderDisplayName'),
            col('FolderObjectId'),
            col('GatewayClusterId'),
            col('GatewayId'),
            col('GatewayStatus'),
            col('GatewayType'),
            col('ImportDisplayName'),
            col('ImportId'),
            col('ImportSource'),
            col('ImportType'),
            col('IncludeExpressions'),
            col('IncludeSubartifacts'),
            col('IsSuccess'),
            col('IsTemplateAppFromMarketplace'),
            col('IsTenantAdminApi'),
            col('IsUpdateAppActivity'),
            col('ItemName'),
            col('LabelEventType'),
            col('LastRefreshTime'),
            col('Lineage'),
            col('ModelId'),
            col('ModelsSnapshots'),
            col('Monikers'),
            col('ObjectDisplayName'),
            col('ObjectId'),
            col('ObjectType'),
            col('OldSensitivityLabelId'),
            col('OrgAppPermission.permissions').alias('OrgAppPermissionPermissions'),
            col('OrgAppPermission.recipients').alias('OrgAppPermissionRecipients'),
            col('OrganizationId'),
            col('PackageId'),
            col('RecordType'),
            col('RefreshEnforcementPolicy'),
            col('RefreshType'),
            col('ReportId'),
            col('ReportName'),
            col('ReportType'),
            col('RequestId'),
            col('RequiredWorkspaces'),
            col('ResultStatus'),
            col('Schedules.Days').alias('SchedulesDays'),
            col('Schedules.RefreshFrequency').alias('SchedulesRefreshFrequency'),
            col('Schedules.Time').alias('SchedulesTime'),
            col('Schedules.TimeZone').alias('SchedulesTimeZone'),
            col('SensitivityLabelEventData.ActionSource').alias('SensitivityLabelEventDataActionSource'),
            col('SensitivityLabelEventData.ActionSourceDetail').alias('SensitivityLabelEventActionSourceDetail'),
            col('SensitivityLabelEventData.LabelEventType').alias('SensitivityLabelEventDataLabelEventType'),
            col('SensitivityLabelEventData.OldSensitivityLabelId').alias('SensitivityLabelEventDataOldSensitivityLabelId'),
            col('SensitivityLabelEventData.SensitivityLabelId').alias('SensitivityLabelEventDataSensitivityLabelId'),
            col('SensitivityLabelId'),
            col('ShareLinkId'),
            col('SharingAction'),
            col('SharingScope'),
            col('SingleSignOnType'),
            col('SwitchState'),
            col('TargetWorkspaceId'),
            col('TemplateAppFolderObjectId'),
            col('TemplateAppIsInstalledWithAutomation'),
            col('TemplateAppObjectId'),
            col('TemplateAppOwnerTenantObjectId'),
            col('TemplateAppVersion'),
            col('TemplatePackageName'),
            col('TileText'),
            col('UserAgent'),
            col('UserId'),
            col('UserKey'),
            col('UserType'),
            col('WorkSpaceName'),
            col('Workload'),
            col('WorkspaceId'),
            col('WorkspacesModifiedSince'),
            col('WorkspacesSemicolonDelimitedList')
        )