/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetadataManagerStats
{
    private final AtomicLong applyTableFunctionCalls = new AtomicLong();
    private final AtomicLong verifyComparableOrderableContractCalls = new AtomicLong();
    private final AtomicLong getTypeCalls = new AtomicLong();
    private final AtomicLong registerBuiltInFunctionsCalls = new AtomicLong();
    private final AtomicLong registerConnectorFunctionsCalls = new AtomicLong();
    private final AtomicLong listSchemaNamesCalls = new AtomicLong();
    private final AtomicLong getSchemaPropertiesCalls = new AtomicLong();
    private final AtomicLong getSystemTableCalls = new AtomicLong();
    private final AtomicLong getHandleVersionCalls = new AtomicLong();
    private final AtomicLong getTableHandleForStatisticsCollectionCalls = new AtomicLong();
    private final AtomicLong getLayoutCalls = new AtomicLong();
    private final AtomicLong getAlternativeTableHandleCalls = new AtomicLong();
    private final AtomicLong isLegacyGetLayoutSupportedCalls = new AtomicLong();
    private final AtomicLong getCommonPartitioningCalls = new AtomicLong();
    private final AtomicLong isRefinedPartitioningOverCalls = new AtomicLong();
    private final AtomicLong getPartitioningHandleForExchangeCalls = new AtomicLong();
    private final AtomicLong getInfoCalls = new AtomicLong();
    private final AtomicLong getTableMetadataCalls = new AtomicLong();
    private final AtomicLong listTablesCalls = new AtomicLong();
    private final AtomicLong getColumnHandlesCalls = new AtomicLong();
    private final AtomicLong getColumnMetadataCalls = new AtomicLong();
    private final AtomicLong toExplainIOConstraintsCalls = new AtomicLong();
    private final AtomicLong listTableColumnsCalls = new AtomicLong();
    private final AtomicLong createSchemaCalls = new AtomicLong();
    private final AtomicLong dropSchemaCalls = new AtomicLong();
    private final AtomicLong renameSchemaCalls = new AtomicLong();
    private final AtomicLong createTableCalls = new AtomicLong();
    private final AtomicLong createTemporaryTableCalls = new AtomicLong();
    private final AtomicLong dropTableCalls = new AtomicLong();
    private final AtomicLong truncateTableCalls = new AtomicLong();
    private final AtomicLong getNewTableLayoutCalls = new AtomicLong();
    private final AtomicLong beginCreateTableCalls = new AtomicLong();
    private final AtomicLong finishCreateTableCalls = new AtomicLong();
    private final AtomicLong getInsertLayoutCalls = new AtomicLong();
    private final AtomicLong getStatisticsCollectionMetadataForWriteCalls = new AtomicLong();
    private final AtomicLong getStatisticsCollectionMetadataCalls = new AtomicLong();
    private final AtomicLong beginStatisticsCollectionCalls = new AtomicLong();
    private final AtomicLong finishStatisticsCollectionCalls = new AtomicLong();
    private final AtomicLong beginQueryCalls = new AtomicLong();
    private final AtomicLong cleanupQueryCalls = new AtomicLong();
    private final AtomicLong beginInsertCalls = new AtomicLong();
    private final AtomicLong finishInsertCalls = new AtomicLong();
    private final AtomicLong getDeleteRowIdColumnCalls = new AtomicLong();
    private final AtomicLong getUpdateRowIdColumnCalls = new AtomicLong();
    private final AtomicLong supportsMetadataDeleteCalls = new AtomicLong();
    private final AtomicLong metadataDeleteCalls = new AtomicLong();
    private final AtomicLong beginDeleteCalls = new AtomicLong();
    private final AtomicLong finishDeleteWithOutputCalls = new AtomicLong();
    private final AtomicLong beginCallDistributedProcedureCalls = new AtomicLong();
    private final AtomicLong finishCallDistributedProcedureCalls = new AtomicLong();
    private final AtomicLong beginUpdateCalls = new AtomicLong();
    private final AtomicLong finishUpdateCalls = new AtomicLong();
    private final AtomicLong getRowChangeParadigmCalls = new AtomicLong();
    private final AtomicLong getMergeTargetTableRowIdColumnHandleCalls = new AtomicLong();
    private final AtomicLong beginMergeCalls = new AtomicLong();
    private final AtomicLong finishMergeCalls = new AtomicLong();
    private final AtomicLong getCatalogHandleCalls = new AtomicLong();
    private final AtomicLong getCatalogNamesCalls = new AtomicLong();
    private final AtomicLong listViewsCalls = new AtomicLong();
    private final AtomicLong getViewsCalls = new AtomicLong();
    private final AtomicLong createViewCalls = new AtomicLong();
    private final AtomicLong renameViewCalls = new AtomicLong();
    private final AtomicLong dropViewCalls = new AtomicLong();
    private final AtomicLong createMaterializedViewCalls = new AtomicLong();
    private final AtomicLong dropMaterializedViewCalls = new AtomicLong();
    private final AtomicLong listMaterializedViewsCalls = new AtomicLong();
    private final AtomicLong getMaterializedViewsCalls = new AtomicLong();
    private final AtomicLong beginRefreshMaterializedViewCalls = new AtomicLong();
    private final AtomicLong finishRefreshMaterializedViewCalls = new AtomicLong();
    private final AtomicLong getReferencedMaterializedViewsCalls = new AtomicLong();
    private final AtomicLong getMaterializedViewStatusCalls = new AtomicLong();
    private final AtomicLong resolveIndexCalls = new AtomicLong();
    private final AtomicLong createRoleCalls = new AtomicLong();
    private final AtomicLong dropRoleCalls = new AtomicLong();
    private final AtomicLong listRolesCalls = new AtomicLong();
    private final AtomicLong listRoleGrantsCalls = new AtomicLong();
    private final AtomicLong grantRolesCalls = new AtomicLong();
    private final AtomicLong revokeRolesCalls = new AtomicLong();
    private final AtomicLong listApplicableRolesCalls = new AtomicLong();
    private final AtomicLong listEnabledRolesCalls = new AtomicLong();
    private final AtomicLong grantTablePrivilegesCalls = new AtomicLong();
    private final AtomicLong revokeTablePrivilegesCalls = new AtomicLong();
    private final AtomicLong listTablePrivilegesCalls = new AtomicLong();
    private final AtomicLong commitPageSinkAsyncCalls = new AtomicLong();
    private final AtomicLong getFunctionAndTypeManagerCalls = new AtomicLong();
    private final AtomicLong getProcedureRegistryCalls = new AtomicLong();
    private final AtomicLong getBlockEncodingSerdeCalls = new AtomicLong();
    private final AtomicLong getSessionPropertyManagerCalls = new AtomicLong();
    private final AtomicLong getSchemaPropertyManagerCalls = new AtomicLong();
    private final AtomicLong getTablePropertyManagerCalls = new AtomicLong();
    private final AtomicLong getColumnPropertyManagerCalls = new AtomicLong();
    private final AtomicLong getAnalyzePropertyManagerCalls = new AtomicLong();
    private final AtomicLong getMetadataResolverCalls = new AtomicLong();
    private final AtomicLong getConnectorCapabilitiesCalls = new AtomicLong();
    private final AtomicLong dropBranchCalls = new AtomicLong();
    private final AtomicLong createBranchCalls = new AtomicLong();
    private final AtomicLong dropTagCalls = new AtomicLong();
    private final AtomicLong dropConstraintCalls = new AtomicLong();
    private final AtomicLong addConstraintCalls = new AtomicLong();
    private final AtomicLong renameTableCalls = new AtomicLong();
    private final AtomicLong setTablePropertiesCalls = new AtomicLong();
    private final AtomicLong addColumnCalls = new AtomicLong();
    private final AtomicLong dropColumnCalls = new AtomicLong();
    private final AtomicLong renameColumnCalls = new AtomicLong();
    private final AtomicLong normalizeIdentifierCalls = new AtomicLong();
    private final AtomicLong getTableLayoutFilterCoverageCalls = new AtomicLong();
    private final AtomicLong getTableStatisticsCalls = new AtomicLong();
    private final AtomicLong getCatalogNamesWithConnectorContextCalls = new AtomicLong();
    private final AtomicLong isPushdownSupportedForFilterCalls = new AtomicLong();
    private final TimeStat applyTableFunctionTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat verifyComparableOrderableContractTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTypeTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat registerBuiltInFunctionsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat registerConnectorFunctionsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listSchemaNamesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getSchemaPropertiesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getSystemTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getHandleVersionTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTableHandleForStatisticsCollectionTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getLayoutTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getAlternativeTableHandleTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat isLegacyGetLayoutSupportedTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getCommonPartitioningTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat isRefinedPartitioningOverTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getPartitioningHandleForExchangeTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getInfoTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTableMetadataTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listTablesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getColumnHandlesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getColumnMetadataTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat toExplainIOConstraintsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listTableColumnsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createSchemaTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropSchemaTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat renameSchemaTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createTemporaryTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat truncateTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getNewTableLayoutTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginCreateTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishCreateTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getInsertLayoutTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getStatisticsCollectionMetadataForWriteTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getStatisticsCollectionMetadataTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginStatisticsCollectionTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishStatisticsCollectionTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginQueryTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat cleanupQueryTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginInsertTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishInsertTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getDeleteRowIdColumnTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getUpdateRowIdColumnTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat supportsMetadataDeleteTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat metadataDeleteTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginDeleteTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishDeleteWithOutputTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginCallDistributedProcedureTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishCallDistributedProcedureTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginUpdateTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishUpdateTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getRowChangeParadigmTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getMergeTargetTableRowIdColumnHandleTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginMergeTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishMergeTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getCatalogHandleTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getCatalogNamesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listViewsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getViewsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat renameViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createMaterializedViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropMaterializedViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listMaterializedViewsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getMaterializedViewsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat beginRefreshMaterializedViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat finishRefreshMaterializedViewTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getReferencedMaterializedViewsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getMaterializedViewStatusTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat resolveIndexTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createRoleTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropRoleTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listRolesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listRoleGrantsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat grantRolesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat revokeRolesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listApplicableRolesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listEnabledRolesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat grantTablePrivilegesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat revokeTablePrivilegesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat listTablePrivilegesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat commitPageSinkAsyncTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getFunctionAndTypeManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getProcedureRegistryTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getBlockEncodingSerdeTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getSessionPropertyManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getSchemaPropertyManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTablePropertyManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getColumnPropertyManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getAnalyzePropertyManagerTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getMetadataResolverTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getConnectorCapabilitiesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropBranchTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat createBranchTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropTagTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropConstraintTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat addConstraintTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat renameTableTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat setTablePropertiesTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat addColumnTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat dropColumnTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat renameColumnTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat normalizeIdentifierTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTableLayoutFilterCoverageTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getTableStatisticsTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat getCatalogNamesWithConnectorContextTime = new TimeStat(TimeUnit.NANOSECONDS);
    private final TimeStat isPushdownSupportedForFilterTime = new TimeStat(TimeUnit.NANOSECONDS);

    @Managed
    public long getApplyTableFunctionCalls()
    {
        return applyTableFunctionCalls.get();
    }

    @Managed
    public long getVerifyComparableOrderableContractCalls()
    {
        return verifyComparableOrderableContractCalls.get();
    }

    @Managed
    public long getGetTypeCalls()
    {
        return getTypeCalls.get();
    }

    @Managed
    public long getRegisterBuiltInFunctionsCalls()
    {
        return registerBuiltInFunctionsCalls.get();
    }

    @Managed
    public long getRegisterConnectorFunctionsCalls()
    {
        return registerConnectorFunctionsCalls.get();
    }

    @Managed
    public long getListSchemaNamesCalls()
    {
        return listSchemaNamesCalls.get();
    }

    @Managed
    public long getGetSchemaPropertiesCalls()
    {
        return getSchemaPropertiesCalls.get();
    }

    @Managed
    public long getGetSystemTableCalls()
    {
        return getSystemTableCalls.get();
    }

    @Managed
    public long getGetHandleVersionCalls()
    {
        return getHandleVersionCalls.get();
    }

    @Managed
    public long getGetTableHandleForStatisticsCollectionCalls()
    {
        return getTableHandleForStatisticsCollectionCalls.get();
    }

    @Managed
    public long getGetLayoutCalls()
    {
        return getLayoutCalls.get();
    }

    @Managed
    public long getGetAlternativeTableHandleCalls()
    {
        return getAlternativeTableHandleCalls.get();
    }

    @Managed
    public long getIsLegacyGetLayoutSupportedCalls()
    {
        return isLegacyGetLayoutSupportedCalls.get();
    }

    @Managed
    public long getGetCommonPartitioningCalls()
    {
        return getCommonPartitioningCalls.get();
    }

    @Managed
    public long getIsRefinedPartitioningOverCalls()
    {
        return isRefinedPartitioningOverCalls.get();
    }

    @Managed
    public long getGetPartitioningHandleForExchangeCalls()
    {
        return getPartitioningHandleForExchangeCalls.get();
    }

    @Managed
    public long getGetInfoCalls()
    {
        return getInfoCalls.get();
    }

    @Managed
    public long getGetTableMetadataCalls()
    {
        return getTableMetadataCalls.get();
    }

    @Managed
    public long getListTablesCalls()
    {
        return listTablesCalls.get();
    }

    @Managed
    public long getGetColumnHandlesCalls()
    {
        return getColumnHandlesCalls.get();
    }

    @Managed
    public long getGetColumnMetadataCalls()
    {
        return getColumnMetadataCalls.get();
    }

    @Managed
    public long getToExplainIOConstraintsCalls()
    {
        return toExplainIOConstraintsCalls.get();
    }

    @Managed
    public long getListTableColumnsCalls()
    {
        return listTableColumnsCalls.get();
    }

    @Managed
    public long getCreateSchemaCalls()
    {
        return createSchemaCalls.get();
    }

    @Managed
    public long getDropSchemaCalls()
    {
        return dropSchemaCalls.get();
    }

    @Managed
    public long getRenameSchemaCalls()
    {
        return renameSchemaCalls.get();
    }

    @Managed
    public long getNormalizeIdentifierCalls()
    {
        return normalizeIdentifierCalls.get();
    }

    @Managed
    public long getGetTableLayoutFilterCoverageCalls()
    {
        return getTableLayoutFilterCoverageCalls.get();
    }

    @Managed
    public long getGetTableStatisticsCalls()
    {
        return getTableStatisticsCalls.get();
    }

    @Managed
    @Nested
    public TimeStat getApplyTableFunctionTime()
    {
        return applyTableFunctionTime;
    }

    @Managed
    @Nested
    public TimeStat getVerifyComparableOrderableContractTime()
    {
        return verifyComparableOrderableContractTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTypeTime()
    {
        return getTypeTime;
    }

    @Managed
    @Nested
    public TimeStat getRegisterBuiltInFunctionsTime()
    {
        return registerBuiltInFunctionsTime;
    }

    @Managed
    @Nested
    public TimeStat getRegisterConnectorFunctionsTime()
    {
        return registerConnectorFunctionsTime;
    }

    @Managed
    @Nested
    public TimeStat getListSchemaNamesTime()
    {
        return listSchemaNamesTime;
    }

    @Managed
    @Nested
    public TimeStat getGetSchemaPropertiesTime()
    {
        return getSchemaPropertiesTime;
    }

    @Managed
    @Nested
    public TimeStat getGetSystemTableTime()
    {
        return getSystemTableTime;
    }

    @Managed
    @Nested
    public TimeStat getGetHandleVersionTime()
    {
        return getHandleVersionTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTableHandleForStatisticsCollectionTime()
    {
        return getTableHandleForStatisticsCollectionTime;
    }

    @Managed
    @Nested
    public TimeStat getGetLayoutTime()
    {
        return getLayoutTime;
    }

    @Managed
    @Nested
    public TimeStat getGetAlternativeTableHandleTime()
    {
        return getAlternativeTableHandleTime;
    }

    @Managed
    @Nested
    public TimeStat getIsLegacyGetLayoutSupportedTime()
    {
        return isLegacyGetLayoutSupportedTime;
    }

    @Managed
    @Nested
    public TimeStat getGetCommonPartitioningTime()
    {
        return getCommonPartitioningTime;
    }

    @Managed
    @Nested
    public TimeStat getIsRefinedPartitioningOverTime()
    {
        return isRefinedPartitioningOverTime;
    }

    @Managed
    @Nested
    public TimeStat getGetPartitioningHandleForExchangeTime()
    {
        return getPartitioningHandleForExchangeTime;
    }

    @Managed
    @Nested
    public TimeStat getGetInfoTime()
    {
        return getInfoTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTableMetadataTime()
    {
        return getTableMetadataTime;
    }

    @Managed
    @Nested
    public TimeStat getListTablesTime()
    {
        return listTablesTime;
    }

    @Managed
    @Nested
    public TimeStat getGetColumnHandlesTime()
    {
        return getColumnHandlesTime;
    }

    @Managed
    @Nested
    public TimeStat getGetColumnMetadataTime()
    {
        return getColumnMetadataTime;
    }

    @Managed
    @Nested
    public TimeStat getToExplainIOConstraintsTime()
    {
        return toExplainIOConstraintsTime;
    }

    @Managed
    @Nested
    public TimeStat getListTableColumnsTime()
    {
        return listTableColumnsTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateSchemaTime()
    {
        return createSchemaTime;
    }

    @Managed
    @Nested
    public TimeStat getDropSchemaTime()
    {
        return dropSchemaTime;
    }

    @Managed
    @Nested
    public TimeStat getRenameSchemaTime()
    {
        return renameSchemaTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateTableTime()
    {
        return createTableTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateTemporaryTableTime()
    {
        return createTemporaryTableTime;
    }

    @Managed
    @Nested
    public TimeStat getDropTableTime()
    {
        return dropTableTime;
    }

    @Managed
    @Nested
    public TimeStat getTruncateTableTime()
    {
        return truncateTableTime;
    }

    @Managed
    @Nested
    public TimeStat getGetNewTableLayoutTime()
    {
        return getNewTableLayoutTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginCreateTableTime()
    {
        return beginCreateTableTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishCreateTableTime()
    {
        return finishCreateTableTime;
    }

    @Managed
    @Nested
    public TimeStat getGetInsertLayoutTime()
    {
        return getInsertLayoutTime;
    }

    @Managed
    @Nested
    public TimeStat getGetStatisticsCollectionMetadataForWriteTime()
    {
        return getStatisticsCollectionMetadataForWriteTime;
    }

    @Managed
    @Nested
    public TimeStat getGetStatisticsCollectionMetadataTime()
    {
        return getStatisticsCollectionMetadataTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginStatisticsCollectionTime()
    {
        return beginStatisticsCollectionTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishStatisticsCollectionTime()
    {
        return finishStatisticsCollectionTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginQueryTime()
    {
        return beginQueryTime;
    }

    @Managed
    @Nested
    public TimeStat getCleanupQueryTime()
    {
        return cleanupQueryTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginInsertTime()
    {
        return beginInsertTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishInsertTime()
    {
        return finishInsertTime;
    }

    @Managed
    @Nested
    public TimeStat getGetDeleteRowIdColumnTime()
    {
        return getDeleteRowIdColumnTime;
    }

    @Managed
    @Nested
    public TimeStat getGetUpdateRowIdColumnTime()
    {
        return getUpdateRowIdColumnTime;
    }

    @Managed
    @Nested
    public TimeStat getSupportsMetadataDeleteTime()
    {
        return supportsMetadataDeleteTime;
    }

    @Managed
    @Nested
    public TimeStat getMetadataDeleteTime()
    {
        return metadataDeleteTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginDeleteTime()
    {
        return beginDeleteTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishDeleteWithOutputTime()
    {
        return finishDeleteWithOutputTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginCallDistributedProcedureTime()
    {
        return beginCallDistributedProcedureTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishCallDistributedProcedureTime()
    {
        return finishCallDistributedProcedureTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginUpdateTime()
    {
        return beginUpdateTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishUpdateTime()
    {
        return finishUpdateTime;
    }

    @Managed
    @Nested
    public TimeStat getGetRowChangeParadigmTime()
    {
        return getRowChangeParadigmTime;
    }

    @Managed
    @Nested
    public TimeStat getGetMergeTargetTableRowIdColumnHandleTime()
    {
        return getMergeTargetTableRowIdColumnHandleTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginMergeTime()
    {
        return beginMergeTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishMergeTime()
    {
        return finishMergeTime;
    }

    @Managed
    @Nested
    public TimeStat getGetCatalogHandleTime()
    {
        return getCatalogHandleTime;
    }

    @Managed
    @Nested
    public TimeStat getGetCatalogNamesTime()
    {
        return getCatalogNamesTime;
    }

    @Managed
    @Nested
    public TimeStat getListViewsTime()
    {
        return listViewsTime;
    }

    @Managed
    @Nested
    public TimeStat getGetViewsTime()
    {
        return getViewsTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateViewTime()
    {
        return createViewTime;
    }

    @Managed
    @Nested
    public TimeStat getRenameViewTime()
    {
        return renameViewTime;
    }

    @Managed
    @Nested
    public TimeStat getDropViewTime()
    {
        return dropViewTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateMaterializedViewTime()
    {
        return createMaterializedViewTime;
    }

    @Managed
    @Nested
    public TimeStat getDropMaterializedViewTime()
    {
        return dropMaterializedViewTime;
    }

    @Managed
    @Nested
    public TimeStat getListMaterializedViewsTime()
    {
        return listMaterializedViewsTime;
    }

    @Managed
    @Nested
    public TimeStat getGetMaterializedViewsTime()
    {
        return getMaterializedViewsTime;
    }

    @Managed
    @Nested
    public TimeStat getBeginRefreshMaterializedViewTime()
    {
        return beginRefreshMaterializedViewTime;
    }

    @Managed
    @Nested
    public TimeStat getFinishRefreshMaterializedViewTime()
    {
        return finishRefreshMaterializedViewTime;
    }

    @Managed
    @Nested
    public TimeStat getGetReferencedMaterializedViewsTime()
    {
        return getReferencedMaterializedViewsTime;
    }

    @Managed
    @Nested
    public TimeStat getGetMaterializedViewStatusTime()
    {
        return getMaterializedViewStatusTime;
    }

    @Managed
    @Nested
    public TimeStat getResolveIndexTime()
    {
        return resolveIndexTime;
    }

    @Managed
    @Nested
    public TimeStat getCreateRoleTime()
    {
        return createRoleTime;
    }

    @Managed
    @Nested
    public TimeStat getDropRoleTime()
    {
        return dropRoleTime;
    }

    @Managed
    @Nested
    public TimeStat getListRolesTime()
    {
        return listRolesTime;
    }

    @Managed
    @Nested
    public TimeStat getListRoleGrantsTime()
    {
        return listRoleGrantsTime;
    }

    @Managed
    @Nested
    public TimeStat getGrantRolesTime()
    {
        return grantRolesTime;
    }

    @Managed
    @Nested
    public TimeStat getRevokeRolesTime()
    {
        return revokeRolesTime;
    }

    @Managed
    @Nested
    public TimeStat getListApplicableRolesTime()
    {
        return listApplicableRolesTime;
    }

    @Managed
    @Nested
    public TimeStat getListEnabledRolesTime()
    {
        return listEnabledRolesTime;
    }

    @Managed
    @Nested
    public TimeStat getGrantTablePrivilegesTime()
    {
        return grantTablePrivilegesTime;
    }

    @Managed
    @Nested
    public TimeStat getRevokeTablePrivilegesTime()
    {
        return revokeTablePrivilegesTime;
    }

    @Managed
    @Nested
    public TimeStat getListTablePrivilegesTime()
    {
        return listTablePrivilegesTime;
    }

    @Managed
    @Nested
    public TimeStat getCommitPageSinkAsyncTime()
    {
        return commitPageSinkAsyncTime;
    }

    @Managed
    @Nested
    public TimeStat getGetFunctionAndTypeManagerTime()
    {
        return getFunctionAndTypeManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetProcedureRegistryTime()
    {
        return getProcedureRegistryTime;
    }

    @Managed
    @Nested
    public TimeStat getGetBlockEncodingSerdeTime()
    {
        return getBlockEncodingSerdeTime;
    }

    @Managed
    @Nested
    public TimeStat getGetSessionPropertyManagerTime()
    {
        return getSessionPropertyManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetSchemaPropertyManagerTime()
    {
        return getSchemaPropertyManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTablePropertyManagerTime()
    {
        return getTablePropertyManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetColumnPropertyManagerTime()
    {
        return getColumnPropertyManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetAnalyzePropertyManagerTime()
    {
        return getAnalyzePropertyManagerTime;
    }

    @Managed
    @Nested
    public TimeStat getGetMetadataResolverTime()
    {
        return getMetadataResolverTime;
    }

    @Managed
    @Nested
    public TimeStat getGetConnectorCapabilitiesTime()
    {
        return getConnectorCapabilitiesTime;
    }

    @Managed
    @Nested
    public TimeStat getDropBranchTime()
    {
        return dropBranchTime;
    }

    @Managed
    @Nested
    public TimeStat getDropTagTime()
    {
        return dropTagTime;
    }

    @Managed
    @Nested
    public TimeStat getDropConstraintTime()
    {
        return dropConstraintTime;
    }

    @Managed
    @Nested
    public TimeStat getAddConstraintTime()
    {
        return addConstraintTime;
    }

    @Managed
    @Nested
    public TimeStat getRenameTableTime()
    {
        return renameTableTime;
    }

    @Managed
    @Nested
    public TimeStat getSetTablePropertiesTime()
    {
        return setTablePropertiesTime;
    }

    @Managed
    @Nested
    public TimeStat getAddColumnTime()
    {
        return addColumnTime;
    }

    @Managed
    @Nested
    public TimeStat getDropColumnTime()
    {
        return dropColumnTime;
    }

    @Managed
    @Nested
    public TimeStat getRenameColumnTime()
    {
        return renameColumnTime;
    }

    @Managed
    @Nested
    public TimeStat getNormalizeIdentifierTime()
    {
        return normalizeIdentifierTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTableLayoutFilterCoverageTime()
    {
        return getTableLayoutFilterCoverageTime;
    }

    @Managed
    @Nested
    public TimeStat getGetTableStatisticsTime()
    {
        return getTableStatisticsTime;
    }

    public void recordApplyTableFunctionCall(long duration)
    {
        applyTableFunctionCalls.incrementAndGet();
        applyTableFunctionTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordVerifyComparableOrderableContractCall(long duration)
    {
        verifyComparableOrderableContractCalls.incrementAndGet();
        verifyComparableOrderableContractTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTypeCall(long duration)
    {
        getTypeCalls.incrementAndGet();
        getTypeTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRegisterBuiltInFunctionsCall(long duration)
    {
        registerBuiltInFunctionsCalls.incrementAndGet();
        registerBuiltInFunctionsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRegisterConnectorFunctionsCall(long duration)
    {
        registerConnectorFunctionsCalls.incrementAndGet();
        registerConnectorFunctionsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListSchemaNamesCall(long duration)
    {
        listSchemaNamesCalls.incrementAndGet();
        listSchemaNamesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetSchemaPropertiesCall(long duration)
    {
        getSchemaPropertiesCalls.incrementAndGet();
        getSchemaPropertiesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetSystemTableCall(long duration)
    {
        getSystemTableCalls.incrementAndGet();
        getSystemTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetHandleVersionCall(long duration)
    {
        getHandleVersionCalls.incrementAndGet();
        getHandleVersionTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTableHandleForStatisticsCollectionCall(long duration)
    {
        getTableHandleForStatisticsCollectionCalls.incrementAndGet();
        getTableHandleForStatisticsCollectionTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetLayoutCall(long duration)
    {
        getLayoutCalls.incrementAndGet();
        getLayoutTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetAlternativeTableHandleCall(long duration)
    {
        getAlternativeTableHandleCalls.incrementAndGet();
        getAlternativeTableHandleTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordIsLegacyGetLayoutSupportedCall(long duration)
    {
        isLegacyGetLayoutSupportedCalls.incrementAndGet();
        isLegacyGetLayoutSupportedTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetCommonPartitioningCall(long duration)
    {
        getCommonPartitioningCalls.incrementAndGet();
        getCommonPartitioningTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordIsRefinedPartitioningOverCall(long duration)
    {
        isRefinedPartitioningOverCalls.incrementAndGet();
        isRefinedPartitioningOverTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetPartitioningHandleForExchangeCall(long duration)
    {
        getPartitioningHandleForExchangeCalls.incrementAndGet();
        getPartitioningHandleForExchangeTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetInfoCall(long duration)
    {
        getInfoCalls.incrementAndGet();
        getInfoTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTableMetadataCall(long duration)
    {
        getTableMetadataCalls.incrementAndGet();
        getTableMetadataTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListTablesCall(long duration)
    {
        listTablesCalls.incrementAndGet();
        listTablesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetColumnHandlesCall(long duration)
    {
        getColumnHandlesCalls.incrementAndGet();
        getColumnHandlesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetColumnMetadataCall(long duration)
    {
        getColumnMetadataCalls.incrementAndGet();
        getColumnMetadataTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordToExplainIOConstraintsCall(long duration)
    {
        toExplainIOConstraintsCalls.incrementAndGet();
        toExplainIOConstraintsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListTableColumnsCall(long duration)
    {
        listTableColumnsCalls.incrementAndGet();
        listTableColumnsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateSchemaCall(long duration)
    {
        createSchemaCalls.incrementAndGet();
        createSchemaTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropSchemaCall(long duration)
    {
        dropSchemaCalls.incrementAndGet();
        dropSchemaTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRenameSchemaCall(long duration)
    {
        renameSchemaCalls.incrementAndGet();
        renameSchemaTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateTableCall(long duration)
    {
        createTableCalls.incrementAndGet();
        createTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateTemporaryTableCall(long duration)
    {
        createTemporaryTableCalls.incrementAndGet();
        createTemporaryTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropTableCall(long duration)
    {
        dropTableCalls.incrementAndGet();
        dropTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordTruncateTableCall(long duration)
    {
        truncateTableCalls.incrementAndGet();
        truncateTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetNewTableLayoutCall(long duration)
    {
        getNewTableLayoutCalls.incrementAndGet();
        getNewTableLayoutTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginCreateTableCall(long duration)
    {
        beginCreateTableCalls.incrementAndGet();
        beginCreateTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishCreateTableCall(long duration)
    {
        finishCreateTableCalls.incrementAndGet();
        finishCreateTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetInsertLayoutCall(long duration)
    {
        getInsertLayoutCalls.incrementAndGet();
        getInsertLayoutTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetStatisticsCollectionMetadataForWriteCall(long duration)
    {
        getStatisticsCollectionMetadataForWriteCalls.incrementAndGet();
        getStatisticsCollectionMetadataForWriteTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetStatisticsCollectionMetadataCall(long duration)
    {
        getStatisticsCollectionMetadataCalls.incrementAndGet();
        getStatisticsCollectionMetadataTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginStatisticsCollectionCall(long duration)
    {
        beginStatisticsCollectionCalls.incrementAndGet();
        beginStatisticsCollectionTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishStatisticsCollectionCall(long duration)
    {
        finishStatisticsCollectionCalls.incrementAndGet();
        finishStatisticsCollectionTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginQueryCall(long duration)
    {
        beginQueryCalls.incrementAndGet();
        beginQueryTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCleanupQueryCall(long duration)
    {
        cleanupQueryCalls.incrementAndGet();
        cleanupQueryTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginInsertCall(long duration)
    {
        beginInsertCalls.incrementAndGet();
        beginInsertTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishInsertCall(long duration)
    {
        finishInsertCalls.incrementAndGet();
        finishInsertTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetDeleteRowIdColumnCall(long duration)
    {
        getDeleteRowIdColumnCalls.incrementAndGet();
        getDeleteRowIdColumnTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetUpdateRowIdColumnCall(long duration)
    {
        getUpdateRowIdColumnCalls.incrementAndGet();
        getUpdateRowIdColumnTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordSupportsMetadataDeleteCall(long duration)
    {
        supportsMetadataDeleteCalls.incrementAndGet();
        supportsMetadataDeleteTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordMetadataDeleteCall(long duration)
    {
        metadataDeleteCalls.incrementAndGet();
        metadataDeleteTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginDeleteCall(long duration)
    {
        beginDeleteCalls.incrementAndGet();
        beginDeleteTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishDeleteWithOutputCall(long duration)
    {
        finishDeleteWithOutputCalls.incrementAndGet();
        finishDeleteWithOutputTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginCallDistributedProcedureCall(long duration)
    {
        beginCallDistributedProcedureCalls.incrementAndGet();
        beginCallDistributedProcedureTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishCallDistributedProcedureCall(long duration)
    {
        finishCallDistributedProcedureCalls.incrementAndGet();
        finishCallDistributedProcedureTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginUpdateCall(long duration)
    {
        beginUpdateCalls.incrementAndGet();
        beginUpdateTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishUpdateCall(long duration)
    {
        finishUpdateCalls.incrementAndGet();
        finishUpdateTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetRowChangeParadigmCall(long duration)
    {
        getRowChangeParadigmCalls.incrementAndGet();
        getRowChangeParadigmTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetMergeTargetTableRowIdColumnHandleCall(long duration)
    {
        getMergeTargetTableRowIdColumnHandleCalls.incrementAndGet();
        getMergeTargetTableRowIdColumnHandleTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginMergeCall(long duration)
    {
        beginMergeCalls.incrementAndGet();
        beginMergeTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishMergeCall(long duration)
    {
        finishMergeCalls.incrementAndGet();
        finishMergeTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetCatalogHandleCall(long duration)
    {
        getCatalogHandleCalls.incrementAndGet();
        getCatalogHandleTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetCatalogNamesCall(long duration)
    {
        getCatalogNamesCalls.incrementAndGet();
        getCatalogNamesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListViewsCall(long duration)
    {
        listViewsCalls.incrementAndGet();
        listViewsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetViewsCall(long duration)
    {
        getViewsCalls.incrementAndGet();
        getViewsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateViewCall(long duration)
    {
        createViewCalls.incrementAndGet();
        createViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRenameViewCall(long duration)
    {
        renameViewCalls.incrementAndGet();
        renameViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropViewCall(long duration)
    {
        dropViewCalls.incrementAndGet();
        dropViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateMaterializedViewCall(long duration)
    {
        createMaterializedViewCalls.incrementAndGet();
        createMaterializedViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropMaterializedViewCall(long duration)
    {
        dropMaterializedViewCalls.incrementAndGet();
        dropMaterializedViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListMaterializedViewsCall(long duration)
    {
        listMaterializedViewsCalls.incrementAndGet();
        listMaterializedViewsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetMaterializedViewsCall(long duration)
    {
        getMaterializedViewsCalls.incrementAndGet();
        getMaterializedViewsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordBeginRefreshMaterializedViewCall(long duration)
    {
        beginRefreshMaterializedViewCalls.incrementAndGet();
        beginRefreshMaterializedViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordFinishRefreshMaterializedViewCall(long duration)
    {
        finishRefreshMaterializedViewCalls.incrementAndGet();
        finishRefreshMaterializedViewTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetReferencedMaterializedViewsCall(long duration)
    {
        getReferencedMaterializedViewsCalls.incrementAndGet();
        getReferencedMaterializedViewsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetMaterializedViewStatusCall(long duration)
    {
        getMaterializedViewStatusCalls.incrementAndGet();
        getMaterializedViewStatusTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordResolveIndexCall(long duration)
    {
        resolveIndexCalls.incrementAndGet();
        resolveIndexTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateRoleCall(long duration)
    {
        createRoleCalls.incrementAndGet();
        createRoleTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropRoleCall(long duration)
    {
        dropRoleCalls.incrementAndGet();
        dropRoleTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListRolesCall(long duration)
    {
        listRolesCalls.incrementAndGet();
        listRolesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListRoleGrantsCall(long duration)
    {
        listRoleGrantsCalls.incrementAndGet();
        listRoleGrantsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGrantRolesCall(long duration)
    {
        grantRolesCalls.incrementAndGet();
        grantRolesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRevokeRolesCall(long duration)
    {
        revokeRolesCalls.incrementAndGet();
        revokeRolesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListApplicableRolesCall(long duration)
    {
        listApplicableRolesCalls.incrementAndGet();
        listApplicableRolesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListEnabledRolesCall(long duration)
    {
        listEnabledRolesCalls.incrementAndGet();
        listEnabledRolesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGrantTablePrivilegesCall(long duration)
    {
        grantTablePrivilegesCalls.incrementAndGet();
        grantTablePrivilegesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRevokeTablePrivilegesCall(long duration)
    {
        revokeTablePrivilegesCalls.incrementAndGet();
        revokeTablePrivilegesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordListTablePrivilegesCall(long duration)
    {
        listTablePrivilegesCalls.incrementAndGet();
        listTablePrivilegesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCommitPageSinkAsyncCall(long duration)
    {
        commitPageSinkAsyncCalls.incrementAndGet();
        commitPageSinkAsyncTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetFunctionAndTypeManagerCall(long duration)
    {
        getFunctionAndTypeManagerCalls.incrementAndGet();
        getFunctionAndTypeManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetProcedureRegistryCall(long duration)
    {
        getProcedureRegistryCalls.incrementAndGet();
        getProcedureRegistryTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetBlockEncodingSerdeCall(long duration)
    {
        getBlockEncodingSerdeCalls.incrementAndGet();
        getBlockEncodingSerdeTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetSessionPropertyManagerCall(long duration)
    {
        getSessionPropertyManagerCalls.incrementAndGet();
        getSessionPropertyManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetSchemaPropertyManagerCall(long duration)
    {
        getSchemaPropertyManagerCalls.incrementAndGet();
        getSchemaPropertyManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTablePropertyManagerCall(long duration)
    {
        getTablePropertyManagerCalls.incrementAndGet();
        getTablePropertyManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetColumnPropertyManagerCall(long duration)
    {
        getColumnPropertyManagerCalls.incrementAndGet();
        getColumnPropertyManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetAnalyzePropertyManagerCall(long duration)
    {
        getAnalyzePropertyManagerCalls.incrementAndGet();
        getAnalyzePropertyManagerTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetMetadataResolverCall(long duration)
    {
        getMetadataResolverCalls.incrementAndGet();
        getMetadataResolverTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetConnectorCapabilitiesCall(long duration)
    {
        getConnectorCapabilitiesCalls.incrementAndGet();
        getConnectorCapabilitiesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropBranchCall(long duration)
    {
        dropBranchCalls.incrementAndGet();
        dropBranchTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordCreateBranchCall(long duration)
    {
        createBranchCalls.incrementAndGet();
        createBranchTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropTagCall(long duration)
    {
        dropTagCalls.incrementAndGet();
        dropTagTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropConstraintCall(long duration)
    {
        dropConstraintCalls.incrementAndGet();
        dropConstraintTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordAddConstraintCall(long duration)
    {
        addConstraintCalls.incrementAndGet();
        addConstraintTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRenameTableCall(long duration)
    {
        renameTableCalls.incrementAndGet();
        renameTableTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordSetTablePropertiesCall(long duration)
    {
        setTablePropertiesCalls.incrementAndGet();
        setTablePropertiesTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordAddColumnCall(long duration)
    {
        addColumnCalls.incrementAndGet();
        addColumnTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordDropColumnCall(long duration)
    {
        dropColumnCalls.incrementAndGet();
        dropColumnTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordRenameColumnCall(long duration)
    {
        renameColumnCalls.incrementAndGet();
        renameColumnTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordNormalizeIdentifierCall(long duration)
    {
        normalizeIdentifierCalls.incrementAndGet();
        normalizeIdentifierTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTableLayoutFilterCoverageCall(long duration)
    {
        getTableLayoutFilterCoverageCalls.incrementAndGet();
        getTableLayoutFilterCoverageTime.add(duration, TimeUnit.NANOSECONDS);
    }

    public void recordGetTableStatisticsCall(long duration)
    {
        getTableStatisticsCalls.incrementAndGet();
        getTableStatisticsTime.add(duration, TimeUnit.NANOSECONDS);
    }

    @Managed
    public long getGetCatalogNamesWithConnectorContextCalls()
    {
        return getCatalogNamesWithConnectorContextCalls.get();
    }

    @Managed
    @Nested
    public TimeStat getGetCatalogNamesWithConnectorContextTime()
    {
        return getCatalogNamesWithConnectorContextTime;
    }

    public void recordGetCatalogNamesWithConnectorContextCall(long duration)
    {
        getCatalogNamesWithConnectorContextCalls.incrementAndGet();
        getCatalogNamesWithConnectorContextTime.add(duration, TimeUnit.NANOSECONDS);
    }

    @Managed
    public long getIsPushdownSupportedForFilterCalls()
    {
        return isPushdownSupportedForFilterCalls.get();
    }

    @Managed
    @Nested
    public TimeStat getIsPushdownSupportedForFilterTime()
    {
        return isPushdownSupportedForFilterTime;
    }

    public void recordIsPushdownSupportedForFilterCall(long duration)
    {
        isPushdownSupportedForFilterCalls.incrementAndGet();
        isPushdownSupportedForFilterTime.add(duration, TimeUnit.NANOSECONDS);
    }
}
