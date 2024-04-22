from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, LongType

class FabricMonitorSchema:

    def __init__(self):
        self.define_schemas()

    def define_schemas(self):

        self.usage_schema = StructType([
            StructField('ActionSource', StringType(), True),
            StructField('ActionSourceDetail', StringType(), True),
            StructField('Activity', StringType(), True),
            StructField('ActivityId', StringType(), True),
            StructField('AggregatedWorkspaceInformation',
                StructType([
                    StructField('WorkspaceCount', LongType(), True),
                    StructField('WorkspacesByCapacitySku', StringType(), True),
                    StructField('WorkspacesByType', StringType(), True)
                ]), True),
            StructField('AppId', StringType(), True),
            StructField('AppName', StringType(), True),
            StructField('AppReportId', StringType(), True),
            StructField('ArtifactAccessRequestInfo',
                StructType([
                    StructField('AccessRequestAction', StringType(), True),
                    StructField('ArtifactLocationObjectId', StringType(), True),
                    StructField('ArtifactOwnerInformation',
                        ArrayType(
                            StructType([
                                StructField('EmailAddress', StringType(), True),
                                StructField('UserObjectId', StringType(), True)
                            ]), True
                        ), True
                    ),
                    StructField('RequestId', LongType(), True),
                    StructField('RequesterUserObjectId', StringType(), True),
                    StructField('TenantObjectId', StringType(), True),
                    StructField('WorkspaceName', StringType(), True)
                ]), True
            ),
            StructField('ArtifactId', StringType(), True),
            StructField('ArtifactKind', StringType(), True),
            StructField('ArtifactName', StringType(), True),
            StructField('ArtifactType', StringType(), True),
            StructField('CapacityId', StringType(), True),
            StructField('CapacityName', StringType(), True),
            StructField('CapacityState', StringType(), True),
            StructField('CapacityUsers', StringType(), True),
            StructField('ClientIP', StringType(), True),
            StructField('ConsumptionMethod', StringType(), True),
            StructField('CopiedReportId', StringType(), True),
            StructField('CopiedReportName', StringType(), True),
            StructField('CreationTime', StringType(), True),
            StructField('CustomVisualAccessTokenResourceId', StringType(), True),
            StructField('CustomVisualAccessTokenSiteUri', StringType(), True),
            StructField('DashboardId', StringType(), True),
            StructField('DashboardName', StringType(), True),
            StructField('DataClassification', StringType(), True),
            StructField('DataConnectivityMode', StringType(), True),
            StructField('DataflowAccessTokenRequestParameters',
                StructType([
                    StructField('entityName', StringType(), True),
                    StructField('partitionUri', StringType(), True),
                    StructField('permissions', LongType(), True),
                    StructField('tokenLifetimeInMinutes', LongType(), True)
                ]), True
            ),
            StructField('DataflowAllowNativeQueries', BooleanType(), True),
            StructField('DataflowId', StringType(), True),
            StructField('DataflowName', StringType(), True),
            StructField('DataflowRefreshScheduleType', StringType(), True),
            StructField('DataflowType', StringType(), True),
            StructField('DatasetId', StringType(), True),
            StructField('DatasetName', StringType(), True),
            StructField('Datasets',
                ArrayType(
                    StructType([
                        StructField('DatasetId', StringType(), True),
                        StructField('DatasetName', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('DatasourceDetails', BooleanType(), True),
            StructField('DatasourceId', StringType(), True),
            StructField('DatasourceObjectIds', ArrayType(StringType(), True), True),
            StructField('DeploymentPipelineId', LongType(), True),
            StructField('DeploymentPipelineObjectId', StringType(), True),
            StructField('DistributionMethod', StringType(), True),
            StructField('EmbedTokenId', StringType(), True),
            StructField('EndPoint', StringType(), True),
            StructField('ExcludePersonalWorkspaces', BooleanType(), True),
            StructField('Experience', StringType(), True),
            StructField('ExportEventEndDateTimeParameter', StringType(), True),
            StructField('ExportEventStartDateTimeParameter', StringType(), True),
            StructField('ExportedArtifactInfo',
                    StructType([
                    StructField('ArtifactId', LongType(), True),
                    StructField('ArtifactType', StringType(), True),
                    StructField('ExportType', StringType(), True)
                ]), True
            ),
            StructField('FolderAccessRequests',
                ArrayType(
                    StructType([
                        StructField('GroupId', LongType(), True),
                        StructField('GroupObjectId', StringType(), True),
                        StructField('RolePermissions', StringType(), True),
                        StructField('UserId', LongType(), True),
                        StructField('UserObjectId', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('FolderDisplayName', StringType(), True),
            StructField('FolderObjectId', StringType(), True),
            StructField('GatewayClusterDatasources',
                ArrayType(
                    StructType([
                        StructField('clusterId', StringType(), True),
                        StructField('credentialDetails',
                            StructType([
                                StructField('credentialType', StringType(), True),
                                StructField('encryptedConnection', StringType(), True),
                                StructField('encryptionAlgorithm', StringType(), True),
                                StructField('isCredentialEncrypted', BooleanType(), True),
                                StructField('privacyLevel', StringType(), True),
                                StructField('skipTestConnection', BooleanType(), True),
                                StructField('useCustomOAuthApp', BooleanType(), True)
                            ]), True
                        ),
                        StructField('credentialType', StringType(), True),
                        StructField('datasourceName', StringType(), True),
                        StructField('datasourceType', StringType(), True),
                        StructField('gatewayClusterName', StringType(), True),
                        StructField('id', StringType(), True),
                        StructField('users',
                            ArrayType(
                                StructType([
                                    StructField('identifier', StringType(), True)
                                ]), True
                            ), True
                        )
                    ]), True
                ), True
            ),
            StructField('GatewayClusterId', StringType(), True),
            StructField('GatewayClustersObjectIds', ArrayType(StringType(), True), True),
            StructField('GatewayId', StringType(), True),
            StructField('GatewayStatus', StringType(), True),
            StructField('GatewayType', StringType(), True),
            StructField('Id', StringType(), False),
            StructField('ImportDisplayName', StringType(), True),
            StructField('ImportId', StringType(), True),
            StructField('ImportSource', StringType(), True),
            StructField('ImportType', StringType(), True),
            StructField('IncludeExpressions', BooleanType(), True),
            StructField('IncludeSubartifacts', BooleanType(), True),
            StructField('InstallTeamsAnalyticsInformation',
                StructType([
                    StructField('ModelId', StringType(), True),
                    StructField('TenantId', StringType(), True),
                    StructField('UserId', StringType(), True)
                ]), True
            ),
            StructField('IsSuccess', BooleanType(), True),
            StructField('IsTemplateAppFromMarketplace', BooleanType(), True),
            StructField('IsTenantAdminApi', BooleanType(), True),
            StructField('IsUpdateAppActivity', BooleanType(), True),
            StructField('ItemName', StringType(), True),
            StructField('LabelEventType', StringType(), True),
            StructField('LastRefreshTime', StringType(), True),
            StructField('Lineage', BooleanType(), True),
            StructField('ModelId', StringType(), True),
            StructField('ModelsSnapshots', ArrayType(LongType(), True), True),
            StructField('Monikers', ArrayType(StringType(), True), True),
            StructField('ObjectDisplayName', StringType(), True),
            StructField('ObjectId', StringType(), True),
            StructField('ObjectType', StringType(), True),
            StructField('OldSensitivityLabelId', StringType(), True),
            StructField('Operation', StringType(), True),
            StructField('OrgAppPermission',
                StructType([
                    StructField('permissions', StringType(), True),
                    StructField('recipients', StringType(), True)
                ]), True
            ),
            StructField('OrganizationId', StringType(), True),
            StructField('PackageId', LongType(), True),
            StructField('PaginatedReportDataSources',
                ArrayType(
                    StructType([
                        StructField('connectionString', StringType(), True),
                        StructField('credentialRetrievalType', StringType(), True),
                        StructField('dMMoniker', StringType(), True),
                        StructField('name', StringType(), True),
                        StructField('provider', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('PinReportToTabInformation',
                StructType([
                    StructField('UserId', StringType(), True)
                ]), True
            ),
            StructField('RecordType', LongType(), True),
            StructField('RefreshEnforcementPolicy', LongType(), True),
            StructField('RefreshType', StringType(), True),
            StructField('ReportId', StringType(), True),
            StructField('ReportName', StringType(), True),
            StructField('ReportType', StringType(), True),
            StructField('RequestId', StringType(), True),
            StructField('RequiredWorkspaces', ArrayType(StringType(), True), True),
            StructField('ResultStatus', StringType(), True),
            StructField('Schedules',
                StructType([
                    StructField('Days', ArrayType(StringType(), True), True),
                    StructField('RefreshFrequency', StringType(), True),
                    StructField('Time', ArrayType(StringType(), True), True),
                    StructField('TimeZone', StringType(), True)
                ]), True
            ),
            StructField('SensitivityLabelEventData',
                StructType([
                    StructField('ActionSource', StringType(), True),
                    StructField('ActionSourceDetail', StringType(), True),
                    StructField('LabelEventType', StringType(), True),
                    StructField('OldSensitivityLabelId', StringType(), True),
                    StructField('SensitivityLabelId', StringType(), True)
                ]), True
            ),
            StructField('SensitivityLabelId', StringType(), True),
            StructField('ShareLinkId', StringType(), True),
            StructField('SharingAction', StringType(), True),
            StructField('SharingInformation',
                ArrayType(
                    StructType([
                        StructField('ObjectId', StringType(), True),
                        StructField('RecipientEmail', StringType(), True),
                        StructField('RecipientName', StringType(), True),
                        StructField('ResharePermission', StringType(), True),
                        StructField('TenantObjectId', StringType(), True),
                        StructField('UserPrincipalName', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('SharingScope', StringType(), True),
            StructField('SingleSignOnType', StringType(), True),
            StructField('SwitchState', StringType(), True),
            StructField('TargetWorkspaceId', StringType(), True),
            StructField('TemplateAppFolderObjectId', StringType(), True),
            StructField('TemplateAppIsInstalledWithAutomation', BooleanType(), True),
            StructField('TemplateAppObjectId', StringType(), True),
            StructField('TemplateAppOwnerTenantObjectId', StringType(), True),
            StructField('TemplateAppVersion', StringType(), True),
            StructField('TemplatePackageName', StringType(), True),
            StructField('TileText', StringType(), True),
            StructField('UpdateFeaturedTables',
                ArrayType(
                    StructType([
                        StructField('State', StringType(), True),
                        StructField('TableName', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('UserAgent', StringType(), True),
            StructField('UserId', StringType(), True),
            StructField('UserKey', StringType(), True),
            StructField('UserType', LongType(), True),
            StructField('WorkSpaceName', StringType(), True),
            StructField('Workload', StringType(), True),
            StructField('WorkspaceAccessList',
                ArrayType(
                    StructType([
                        StructField('UserAccessList',
                            ArrayType(
                                StructType([
                                    StructField('GroupUserAccessRight', StringType(), True),
                                    StructField('Identifier', StringType(), True),
                                    StructField('PrincipalType', StringType(), True),
                                    StructField('UserEmailAddress', StringType(), True)
                                ]), True
                            ), True
                        ),
                        StructField('WorkspaceId', StringType(), True)
                    ]), True
                ), True
            ),
            StructField('WorkspaceId', StringType(), True),
            StructField('WorkspacesModifiedSince', StringType(), True),
            StructField('WorkspacesSemicolonDelimitedList', StringType(), True)
        ])

        self.catalog_schema = StructType(
            [
                StructField(
                    'datasourceInstances',
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    'connectionDetails',
                                    StructType(
                                        [
                                            StructField('account', StringType(), True),
                                            StructField('classInfo', StringType(), True),
                                            StructField('connectionString', StringType(), True),
                                            StructField('database', StringType(), True),
                                            StructField('domain', StringType(), True),
                                            StructField('emailAddress', StringType(), True),
                                            StructField('extensionDataSourceKind', StringType(), True),
                                            StructField('extensionDataSourcePath', StringType(), True),
                                            StructField('loginServer', StringType(), True),
                                            StructField('path', StringType(), True),
                                            StructField('server', StringType(), True),
                                            StructField('sharePointSiteUrl', StringType(), True),
                                            StructField('url', StringType(), True),
                                        ]
                                    ), True
                                ),
                                StructField('datasourceId', StringType(), True),
                                StructField('datasourceType', StringType(), True),
                                StructField('gatewayId', StringType(), True)
                            ]
                        ), True
                    ), True
                ),
                StructField(
                    'misconfiguredDatasourceInstances',
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    'connectionDetails',
                                    StructType(
                                        [
                                            StructField('connectionString', StringType(), True),
                                            StructField('database', StringType(), True),
                                            StructField('domain', StringType(), True),
                                            StructField('kind', StringType(), True),
                                            StructField('path', StringType(), True),
                                            StructField('server', StringType(), True),
                                            StructField('url', StringType(), True),
                                        ]
                                    ), True
                                ),
                                StructField('datasourceId', StringType(), True),
                                StructField('datasourceType', StringType(), True)
                            ]
                        ), True
                    ), True
                ),
                StructField(
                    'workspaces',
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    'DataPipeline',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'endorsementDetails',
                                                    StructType(
                                                        [
                                                            StructField('certifiedBy', StringType(), True),
                                                            StructField('endorsement', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'Eventstream',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'KQLDatabase',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'extendedProperties',
                                                    StructType(
                                                        [
                                                            StructField('IngestionServiceUri', StringType(), True),
                                                            StructField('KustoDatabaseType', StringType(), True),
                                                            StructField('QueryServiceUri', StringType(), True),
                                                            StructField('Region', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'KQLQueryset',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'Lakehouse',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'extendedProperties',
                                                    StructType(
                                                        [
                                                            StructField('DwProperties', StringType(), True),
                                                            StructField('OneLakeFilesPath', StringType(), True),
                                                            StructField('OneLakeTablesPath', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'MLExperiment',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'extendedProperties',
                                                    StructType(
                                                        [
                                                            StructField('AmlExperimentId', StringType(), True),
                                                            StructField('MLFlowExperimentId', StringType(), True)
                                                        ]
                                                    ),
                                                    True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'MLModel',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'Notebook',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'endorsementDetails',
                                                    StructType(
                                                        [
                                                            StructField('certifiedBy', StringType(), True),
                                                            StructField('endorsement', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'SQLEndpoints',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('configuredBy', StringType(), True),
                                                StructField('configuredById', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('modifiedDateTime', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datamartUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'SparkJobDefinition',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'extendedProperties',
                                                    StructType(
                                                        [
                                                            StructField('OneLakeRootPath', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField('capacityId', StringType(), True),
                                StructField(
                                    'dashboards',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('appId', StringType(), True),
                                                StructField('displayName', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('isReadOnly', BooleanType(), True),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'tiles',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasetId', StringType(), True),
                                                                StructField('datasetWorkspaceId', StringType(), True),
                                                                StructField('id', StringType(), True),
                                                                StructField('reportId', StringType(), True),
                                                                StructField('subTitle', StringType(), True),
                                                                StructField('title', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dashboardUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField('dataRetrievalState', StringType(), True),
                                StructField(
                                    'dataflows',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('configuredBy', StringType(), True),
                                                StructField(
                                                    'datasourceUsages',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasourceInstanceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'endorsementDetails',
                                                    StructType(
                                                        [
                                                            StructField('endorsement', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedDateTime', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('objectId', StringType(), True),
                                                StructField(
                                                    'refreshSchedule',
                                                    StructType(
                                                        [
                                                            StructField('days', ArrayType(StringType(), True), True),
                                                            StructField('enabled', BooleanType(), True),
                                                            StructField('localTimeZoneId', StringType(), True),
                                                            StructField('notifyOption', StringType(), True),
                                                            StructField('times', ArrayType(StringType(), True), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dataflowUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'datamarts',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('configuredBy', StringType(), True),
                                                StructField('configuredById', StringType(), True),
                                                StructField(
                                                    'datasourceUsages',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasourceInstanceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('description', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('modifiedDateTime', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'refreshSchedule',
                                                    StructType(
                                                        [
                                                            StructField('days', ArrayType(StringType(), True), True),
                                                            StructField('enabled', BooleanType(), True),
                                                            StructField('localTimeZoneId', StringType(), True),
                                                            StructField('notifyOption', StringType(), True),
                                                            StructField('times', ArrayType(StringType(), True), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('type', StringType(), True),
                                                StructField(
                                                    'upstreamDataflows',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('groupId', StringType(), True),
                                                                StructField('targetDataflowId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'upstreamDatamarts',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('groupId', StringType(), True),
                                                                StructField('targetDatamartId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datamartUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'datasets',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('configuredBy', StringType(), True),
                                                StructField('configuredById', StringType(), True),
                                                StructField('contentProviderType', StringType(), True),
                                                StructField('createdDate', StringType(), True),
                                                StructField(
                                                    'datasourceUsages',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasourceInstanceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'directQueryRefreshSchedule',
                                                    StructType(
                                                        [
                                                            StructField('days', ArrayType(StringType(), True), True),
                                                            StructField('frequency', LongType(), True),
                                                            StructField('localTimeZoneId', StringType(), True),
                                                            StructField('times', ArrayType(StringType(), True), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'endorsementDetails',
                                                    StructType(
                                                        [
                                                            StructField('certifiedBy', StringType(), True),
                                                            StructField('endorsement', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'expressions',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('description', StringType(), True),
                                                                StructField('expression', StringType(), True),
                                                                StructField('name', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('isEffectiveIdentityRequired', BooleanType(), True),
                                                StructField('isEffectiveIdentityRolesRequired', BooleanType(), True),
                                                StructField(
                                                    'misconfiguredDatasourceUsages',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasourceInstanceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'refreshSchedule',
                                                    StructType(
                                                        [
                                                            StructField('days', ArrayType(StringType(), True), True),
                                                            StructField('enabled', BooleanType(), True),
                                                            StructField('localTimeZoneId', StringType(), True),
                                                            StructField('notifyOption', StringType(), True),
                                                            StructField('times', ArrayType(StringType(), True), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'roles',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    'members',
                                                                    ArrayType(
                                                                        StructType(
                                                                            [
                                                                                StructField('identityProvider', StringType(), True),
                                                                                StructField('memberId', StringType(), True),
                                                                                StructField('memberName', StringType(), True),
                                                                                StructField('memberType', StringType(), True)
                                                                            ]
                                                                        ), True
                                                                    ), True
                                                                ),
                                                                StructField('modelPermission', StringType(), True),
                                                                StructField('name', StringType(), True),
                                                                StructField(
                                                                    'tablePermissions',
                                                                    ArrayType(
                                                                        StructType(
                                                                            [
                                                                                StructField('filterExpression', StringType(), True),
                                                                                StructField('name', StringType(), True)
                                                                            ]
                                                                        ), True
                                                                    ), True
                                                                )
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('schemaMayNotBeUpToDate', BooleanType(), True),
                                                StructField('schemaRetrievalError', StringType(), True),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'tables',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    'columns',
                                                                    ArrayType(
                                                                        StructType(
                                                                            [
                                                                                StructField('columnType', StringType(), True),
                                                                                StructField('dataType', StringType(), True),
                                                                                StructField('description', StringType(), True),
                                                                                StructField('expression', StringType(), True),
                                                                                StructField('isHidden', BooleanType(), True),
                                                                                StructField('name', StringType(), True)
                                                                            ]
                                                                        ), True
                                                                    ), True
                                                                ),
                                                                StructField('description', StringType(), True),
                                                                StructField('isHidden', BooleanType(), True),
                                                                StructField(
                                                                    'measures',
                                                                    ArrayType(
                                                                        StructType(
                                                                            [
                                                                                StructField('description', StringType(), True),
                                                                                StructField('expression', StringType(), True),
                                                                                StructField('isHidden', BooleanType(), True),
                                                                                StructField('name', StringType(), True)
                                                                            ]
                                                                        ), True
                                                                    ), True
                                                                ),
                                                                StructField('name', StringType(), True),
                                                                StructField(
                                                                    'source',
                                                                    ArrayType(
                                                                        StructType(
                                                                            [
                                                                                StructField('expression', StringType(), True)
                                                                            ]
                                                                        ), True
                                                                    ), True
                                                                )
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('targetStorageMode', StringType(), True),
                                                StructField(
                                                    'upstreamDataflows',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('groupId', StringType(), True),
                                                                StructField('targetDataflowId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'upstreamDatamarts',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('groupId', StringType(), True),
                                                                StructField('targetDatamartId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'upstreamDatasets',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('groupId', StringType(), True),
                                                                StructField('targetDatasetId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasetUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField('defaultDatasetStorageFormat', StringType(), True),
                                StructField('description', StringType(), True),
                                StructField('id', StringType(), True),
                                StructField('isOnDedicatedCapacity', BooleanType(), True),
                                StructField(
                                    'kustoeventhubdataconnection',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'extendedProperties',
                                                    StructType(
                                                        [
                                                            StructField('DataConnectionType', StringType(), True),
                                                            StructField('DataSourceConnectionId', StringType(), True),
                                                            StructField('TargetDatabaseArtifactId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField('name', StringType(), True),
                                StructField(
                                    'reflexproject',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('description', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('lastUpdatedDate', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('state', StringType(), True),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('artifactUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'reports',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('appId', StringType(), True),
                                                StructField('createdBy', StringType(), True),
                                                StructField('createdById', StringType(), True),
                                                StructField('createdDateTime', StringType(), True),
                                                StructField('datasetId', StringType(), True),
                                                StructField('datasetWorkspaceId', StringType(), True),
                                                StructField(
                                                    'datasourceUsages',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datasourceInstanceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('description', StringType(), True),
                                                StructField(
                                                    'endorsementDetails',
                                                    StructType(
                                                        [
                                                            StructField('certifiedBy', StringType(), True),
                                                            StructField('endorsement', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField('id', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('modifiedDateTime', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField('originalReportObjectId', StringType(), True),
                                                StructField(
                                                    'relations',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('dependentOnArtifactId', StringType(), True),
                                                                StructField('relationType', StringType(), True),
                                                                StructField('settingsList', StringType(), True),
                                                                StructField('usage', StringType(), True),
                                                                StructField('workspaceId', StringType(), True)
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                                StructField('reportType', StringType(), True),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('reportUserAccessRight', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField('state', StringType(), True),
                                StructField('type', StringType(), True),
                                StructField(
                                    'users',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('displayName', StringType(), True),
                                                StructField('emailAddress', StringType(), True),
                                                StructField('graphId', StringType(), True),
                                                StructField('groupUserAccessRight', StringType(), True),
                                                StructField('identifier', StringType(), True),
                                                StructField('principalType', StringType(), True),
                                                StructField('userType', StringType(), True),
                                            ]
                                        ), True
                                    ), True
                                ),
                                StructField(
                                    'warehouses',
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField('configuredBy', StringType(), True),
                                                StructField('configuredById', StringType(), True),
                                                StructField('id', StringType(), True),
                                                StructField('modifiedBy', StringType(), True),
                                                StructField('modifiedById', StringType(), True),
                                                StructField('modifiedDateTime', StringType(), True),
                                                StructField('name', StringType(), True),
                                                StructField(
                                                    'sensitivityLabel',
                                                    StructType(
                                                        [
                                                            StructField('labelId', StringType(), True)
                                                        ]
                                                    ), True
                                                ),
                                                StructField(
                                                    'users',
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField('datamartUserAccessRight', StringType(), True),
                                                                StructField('displayName', StringType(), True),
                                                                StructField('emailAddress', StringType(), True),
                                                                StructField('graphId', StringType(), True),
                                                                StructField('identifier', StringType(), True),
                                                                StructField('principalType', StringType(), True),
                                                                StructField('userType', StringType(), True),
                                                            ]
                                                        ), True
                                                    ), True
                                                ),
                                            ]
                                        ), True
                                    ), True
                                ),
                            ]
                        ), True
                    ), True
                )
            ]
        )