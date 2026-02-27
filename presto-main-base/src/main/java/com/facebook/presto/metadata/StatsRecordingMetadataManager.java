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

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.MergeHandle;
import com.facebook.presto.spi.NewTableLayout;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.procedure.ProcedureRegistry;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class StatsRecordingMetadataManager
        implements Metadata
{
    private final Metadata delegate;
    private final MetadataManagerStats stats;

    public StatsRecordingMetadataManager(Metadata delegate, MetadataManagerStats stats)
    {
        this.delegate = delegate;
        this.stats = stats;
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createSchema(session, schema, properties);
        }
        finally {
            stats.recordCreateSchemaCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropSchema(session, schema);
        }
        finally {
            stats.recordDropSchemaCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        long startTime = System.nanoTime();
        try {
            delegate.renameSchema(session, source, target);
        }
        finally {
            stats.recordRenameSchemaCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createTable(session, catalogName, tableMetadata, ignoreExisting);
        }
        finally {
            stats.recordCreateTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableHandle createTemporaryTable(Session session, String catalogName, List<ColumnMetadata> columns, Optional<PartitioningMetadata> partitioningMetadata)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.createTemporaryTable(session, catalogName, columns, partitioningMetadata);
        }
        finally {
            stats.recordCreateTemporaryTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropTable(session, tableHandle);
        }
        finally {
            stats.recordDropTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            delegate.truncateTable(session, tableHandle);
        }
        finally {
            stats.recordTruncateTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getNewTableLayout(session, catalogName, tableMetadata);
        }
        finally {
            stats.recordGetNewTableLayoutCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginCreateTable(session, catalogName, tableMetadata, layout);
        }
        finally {
            stats.recordBeginCreateTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
        }
        finally {
            stats.recordFinishCreateTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getInsertLayout(session, target);
        }
        finally {
            stats.recordGetInsertLayoutCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata);
        }
        finally {
            stats.recordGetStatisticsCollectionMetadataForWriteCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getStatisticsCollectionMetadata(session, catalogName, tableMetadata);
        }
        finally {
            stats.recordGetStatisticsCollectionMetadataCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginStatisticsCollection(session, tableHandle);
        }
        finally {
            stats.recordBeginStatisticsCollectionCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        long startTime = System.nanoTime();
        try {
            delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
        }
        finally {
            stats.recordFinishStatisticsCollectionCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
        long startTime = System.nanoTime();
        try {
            delegate.beginQuery(session, connectors);
        }
        finally {
            stats.recordBeginQueryCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void cleanupQuery(Session session)
    {
        long startTime = System.nanoTime();
        try {
            delegate.cleanupQuery(session);
        }
        finally {
            stats.recordCleanupQueryCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginInsert(session, tableHandle);
        }
        finally {
            stats.recordBeginInsertCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.finishInsert(session, tableHandle, fragments, computedStatistics);
        }
        finally {
            stats.recordFinishInsertCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ColumnHandle> getDeleteRowIdColumn(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getDeleteRowIdColumn(session, tableHandle);
        }
        finally {
            stats.recordGetDeleteRowIdColumnCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ColumnHandle> getUpdateRowIdColumn(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getUpdateRowIdColumn(session, tableHandle, updatedColumns);
        }
        finally {
            stats.recordGetUpdateRowIdColumnCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.supportsMetadataDelete(session, tableHandle);
        }
        finally {
            stats.recordSupportsMetadataDeleteCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.metadataDelete(session, tableHandle);
        }
        finally {
            stats.recordMetadataDeleteCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public DeleteTableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginDelete(session, tableHandle);
        }
        finally {
            stats.recordBeginDeleteCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishDeleteWithOutput(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.finishDeleteWithOutput(session, tableHandle, fragments);
        }
        finally {
            stats.recordFinishDeleteWithOutputCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public DistributedProcedureHandle beginCallDistributedProcedure(Session session, QualifiedObjectName procedureName, TableHandle tableHandle, Object[] arguments, boolean sourceTableEliminated)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginCallDistributedProcedure(session, procedureName, tableHandle, arguments, sourceTableEliminated);
        }
        finally {
            stats.recordBeginCallDistributedProcedureCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void finishCallDistributedProcedure(Session session, DistributedProcedureHandle procedureHandle, QualifiedObjectName procedureName, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            delegate.finishCallDistributedProcedure(session, procedureHandle, procedureName, fragments);
        }
        finally {
            stats.recordFinishCallDistributedProcedureCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginUpdate(session, tableHandle, updatedColumns);
        }
        finally {
            stats.recordBeginUpdateCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            delegate.finishUpdate(session, tableHandle, fragments);
        }
        finally {
            stats.recordFinishUpdateCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getRowChangeParadigm(session, tableHandle);
        }
        finally {
            stats.recordGetRowChangeParadigmCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ColumnHandle getMergeTargetTableRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getMergeTargetTableRowIdColumnHandle(session, tableHandle);
        }
        finally {
            stats.recordGetMergeTargetTableRowIdColumnHandleCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginMerge(session, tableHandle);
        }
        finally {
            stats.recordBeginMergeCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void finishMerge(Session session, MergeHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        long startTime = System.nanoTime();
        try {
            delegate.finishMerge(session, tableHandle, fragments, computedStatistics);
        }
        finally {
            stats.recordFinishMergeCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getCatalogHandle(session, catalogName);
        }
        finally {
            stats.recordGetCatalogHandleCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getCatalogNames(session);
        }
        finally {
            stats.recordGetCatalogNamesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<String, Catalog.CatalogContext> getCatalogNamesWithConnectorContext(Session session)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getCatalogNamesWithConnectorContext(session);
        }
        finally {
            stats.recordGetCatalogNamesWithConnectorContextCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listViews(session, prefix);
        }
        finally {
            stats.recordListViewsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getViews(session, prefix);
        }
        finally {
            stats.recordGetViewsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createView(session, catalogName, viewMetadata, viewData, replace);
        }
        finally {
            stats.recordCreateViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        long startTime = System.nanoTime();
        try {
            delegate.renameView(session, existingViewName, newViewName);
        }
        finally {
            stats.recordRenameViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropView(session, viewName);
        }
        finally {
            stats.recordDropViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createMaterializedView(session, catalogName, viewMetadata, viewDefinition, ignoreExisting);
        }
        finally {
            stats.recordCreateMaterializedViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropMaterializedView(session, viewName);
        }
        finally {
            stats.recordDropMaterializedViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listMaterializedViews(session, prefix);
        }
        finally {
            stats.recordListMaterializedViewsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<QualifiedObjectName, MaterializedViewDefinition> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getMaterializedViews(session, prefix);
        }
        finally {
            stats.recordGetMaterializedViewsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.beginRefreshMaterializedView(session, tableHandle);
        }
        finally {
            stats.recordBeginRefreshMaterializedViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.finishRefreshMaterializedView(session, tableHandle, fragments, computedStatistics);
        }
        finally {
            stats.recordFinishRefreshMaterializedViewCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getReferencedMaterializedViews(session, tableName);
        }
        finally {
            stats.recordGetReferencedMaterializedViewsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(Session session, QualifiedObjectName viewName, TupleDomain<String> baseQueryDomain)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getMaterializedViewStatus(session, viewName, baseQueryDomain);
        }
        finally {
            stats.recordGetMaterializedViewStatusCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
        }
        finally {
            stats.recordResolveIndexCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createRole(session, role, grantor, catalog);
        }
        finally {
            stats.recordCreateRoleCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropRole(session, role, catalog);
        }
        finally {
            stats.recordDropRoleCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listRoles(session, catalog);
        }
        finally {
            stats.recordListRolesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listRoleGrants(session, catalog, principal);
        }
        finally {
            stats.recordListRoleGrantsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            delegate.grantRoles(session, roles, grantees, withAdminOption, grantor, catalog);
        }
        finally {
            stats.recordGrantRolesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            delegate.revokeRoles(session, roles, grantees, adminOptionFor, grantor, catalog);
        }
        finally {
            stats.recordRevokeRolesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listApplicableRoles(session, principal, catalog);
        }
        finally {
            stats.recordListApplicableRolesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listEnabledRoles(session, catalog);
        }
        finally {
            stats.recordListEnabledRolesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        long startTime = System.nanoTime();
        try {
            delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
        finally {
            stats.recordGrantTablePrivilegesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        long startTime = System.nanoTime();
        try {
            delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
        finally {
            stats.recordRevokeTablePrivilegesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listTablePrivileges(session, prefix);
        }
        finally {
            stats.recordListTablePrivilegesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
        finally {
            stats.recordCommitPageSinkAsyncCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
        finally {
            stats.recordCommitPageSinkAsyncCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
        finally {
            stats.recordCommitPageSinkAsyncCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getFunctionAndTypeManager();
        }
        finally {
            stats.recordGetFunctionAndTypeManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getProcedureRegistry();
        }
        finally {
            stats.recordGetProcedureRegistryCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getBlockEncodingSerde();
        }
        finally {
            stats.recordGetBlockEncodingSerdeCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getSessionPropertyManager();
        }
        finally {
            stats.recordGetSessionPropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getSchemaPropertyManager();
        }
        finally {
            stats.recordGetSchemaPropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getTablePropertyManager();
        }
        finally {
            stats.recordGetTablePropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public MaterializedViewPropertyManager getMaterializedViewPropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getMaterializedViewPropertyManager();
        }
        finally {
            stats.recordGetTablePropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getColumnPropertyManager();
        }
        finally {
            stats.recordGetColumnPropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getAnalyzePropertyManager();
        }
        finally {
            stats.recordGetAnalyzePropertyManagerCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public MetadataResolver getMetadataResolver(Session session)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getMetadataResolver(session);
        }
        finally {
            stats.recordGetMetadataResolverCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getConnectorCapabilities(session, catalogName);
        }
        finally {
            stats.recordGetConnectorCapabilitiesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableLayoutFilterCoverage getTableLayoutFilterCoverage(Session session, TableHandle tableHandle, Set<String> relevantPartitionColumn)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getTableLayoutFilterCoverage(session, tableHandle, relevantPartitionColumn);
        }
        finally {
            stats.recordGetTableLayoutFilterCoverageCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropBranch(Session session, TableHandle tableHandle, String branchName, boolean branchExists)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropBranch(session, tableHandle, branchName, branchExists);
        }
        finally {
            stats.recordDropBranchCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createBranch(Session session,
                      TableHandle tableHandle,
                      String branchName,
                      boolean replace,
                      boolean ifNotExists,
                      Optional<ConnectorTableVersion> tableVersion,
                      Optional<Long> retainDays,
                      Optional<Integer> minSnapshotsToKeep,
                      Optional<Long> maxSnapshotAgeDays)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createBranch(session, tableHandle, branchName, replace, ifNotExists, tableVersion, retainDays, minSnapshotsToKeep, maxSnapshotAgeDays);
        }
        finally {
            stats.recordCreateBranchCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void createTag(Session session,
                          TableHandle tableHandle,
                          String tagName,
                          boolean replace,
                          boolean ifNotExists,
                          Optional<ConnectorTableVersion> tableVersion,
                          Optional<Long> retainDays)
    {
        long startTime = System.nanoTime();
        try {
            delegate.createTag(session, tableHandle, tagName, replace, ifNotExists, tableVersion, retainDays);
        }
        finally {
            stats.recordCreateTagCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropTag(Session session, TableHandle tableHandle, String tagName, boolean tagExists)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropTag(session, tableHandle, tagName, tagExists);
        }
        finally {
            stats.recordDropTagCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropConstraint(Session session, TableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropConstraint(session, tableHandle, constraintName, columnName);
        }
        finally {
            stats.recordDropConstraintCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void addConstraint(Session session, TableHandle tableHandle, TableConstraint<String> tableConstraint)
    {
        long startTime = System.nanoTime();
        try {
            delegate.addConstraint(session, tableHandle, tableConstraint);
        }
        finally {
            stats.recordAddConstraintCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public boolean isPushdownSupportedForFilter(Session session, TableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.isPushdownSupportedForFilter(session, tableHandle, filter, symbolToColumnHandleMap);
        }
        finally {
            stats.recordIsPushdownSupportedForFilterCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        long startTime = System.nanoTime();
        try {
            delegate.renameTable(session, tableHandle, newTableName);
        }
        finally {
            stats.recordRenameTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Object> properties)
    {
        long startTime = System.nanoTime();
        try {
            delegate.setTableProperties(session, tableHandle, properties);
        }
        finally {
            stats.recordSetTablePropertiesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        long startTime = System.nanoTime();
        try {
            delegate.addColumn(session, tableHandle, column);
        }
        finally {
            stats.recordAddColumnCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        long startTime = System.nanoTime();
        try {
            delegate.dropColumn(session, tableHandle, column);
        }
        finally {
            stats.recordDropColumnCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        long startTime = System.nanoTime();
        try {
            delegate.renameColumn(session, tableHandle, source, target);
        }
        finally {
            stats.recordRenameColumnCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public String normalizeIdentifier(Session session, String catalogName, String identifier)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.normalizeIdentifier(session, catalogName, identifier);
        }
        finally {
            stats.recordNormalizeIdentifierCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.applyTableFunction(session, handle);
        }
        finally {
            stats.recordApplyTableFunctionCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        long startTime = System.nanoTime();
        try {
            delegate.verifyComparableOrderableContract();
        }
        finally {
            stats.recordVerifyComparableOrderableContractCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getType(signature);
        }
        finally {
            stats.recordGetTypeCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        long startTime = System.nanoTime();
        try {
            delegate.registerBuiltInFunctions(functions);
        }
        finally {
            stats.recordRegisterBuiltInFunctionsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public void registerConnectorFunctions(String catalogName, List<? extends SqlFunction> functionInfos)
    {
        long startTime = System.nanoTime();
        try {
            delegate.registerConnectorFunctions(catalogName, functionInfos);
        }
        finally {
            stats.recordRegisterConnectorFunctionsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listSchemaNames(session, catalogName);
        }
        finally {
            stats.recordListSchemaNamesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getSchemaProperties(session, schemaName);
        }
        finally {
            stats.recordGetSchemaPropertiesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getSystemTable(session, tableName);
        }
        finally {
            stats.recordGetSystemTableCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<TableHandle> getHandleVersion(Session session, QualifiedObjectName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getHandleVersion(session, tableName, tableVersion);
        }
        finally {
            stats.recordGetHandleVersionCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties);
        }
        finally {
            stats.recordGetTableHandleForStatisticsCollectionCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableLayoutResult getLayout(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getLayout(session, tableHandle, constraint, desiredColumns);
        }
        finally {
            stats.recordGetLayoutCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableLayout getLayout(Session session, TableHandle handle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getLayout(session, handle);
        }
        finally {
            stats.recordGetLayoutCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableHandle getAlternativeTableHandle(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getAlternativeTableHandle(session, tableHandle, partitioningHandle);
        }
        finally {
            stats.recordGetAlternativeTableHandleCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.isLegacyGetLayoutSupported(session, tableHandle);
        }
        finally {
            stats.recordIsLegacyGetLayoutSupportedCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getCommonPartitioning(session, left, right);
        }
        finally {
            stats.recordGetCommonPartitioningCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public boolean isRefinedPartitioningOver(Session session, PartitioningHandle a, PartitioningHandle b)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.isRefinedPartitioningOver(session, a, b);
        }
        finally {
            stats.recordIsRefinedPartitioningOverCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public PartitioningHandle getPartitioningHandleForExchange(Session session, String catalogName, int partitionCount, List<Type> partitionTypes)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getPartitioningHandleForExchange(session, catalogName, partitionCount, partitionTypes);
        }
        finally {
            stats.recordGetPartitioningHandleForExchangeCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getInfo(session, handle);
        }
        finally {
            stats.recordGetInfoCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getTableMetadata(session, tableHandle);
        }
        finally {
            stats.recordGetTableMetadataCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getTableStatistics(session, tableHandle, columnHandles, constraint);
        }
        finally {
            stats.recordGetTableStatisticsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listTables(session, prefix);
        }
        finally {
            stats.recordListTablesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getColumnHandles(session, tableHandle);
        }
        finally {
            stats.recordGetColumnHandlesCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.getColumnMetadata(session, tableHandle, columnHandle);
        }
        finally {
            stats.recordGetColumnMetadataCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.toExplainIOConstraints(session, tableHandle, constraints);
        }
        finally {
            stats.recordToExplainIOConstraintsCall(System.nanoTime() - startTime);
        }
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        long startTime = System.nanoTime();
        try {
            return delegate.listTableColumns(session, prefix);
        }
        finally {
            stats.recordListTableColumnsCall(System.nanoTime() - startTime);
        }
    }
}
