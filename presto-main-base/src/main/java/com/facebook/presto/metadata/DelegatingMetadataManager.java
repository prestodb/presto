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
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MergeHandle;
import com.facebook.presto.spi.NewTableLayout;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class DelegatingMetadataManager
        implements Metadata
{
    private final Metadata delegate;

    @Inject
    public DelegatingMetadataManager(MetadataManager metadataManager)
    {
        this.delegate = requireNonNull(metadataManager, "metadata is null");
    }

    @Override
    public final void verifyComparableOrderableContract()
    {
        delegate.verifyComparableOrderableContract();
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return delegate.getType(signature);
    }

    @Override
    public void registerBuiltInFunctions(List<? extends SqlFunction> functionInfos)
    {
        delegate.registerBuiltInFunctions(functionInfos);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        return delegate.listSchemaNames(session, catalogName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        return delegate.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public Optional<TableHandle> getHandleVersion(Session session, QualifiedObjectName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        return delegate.getHandleVersion(session, tableName, tableVersion);
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties)
    {
        return delegate.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties);
    }

    @Override
    public TableLayoutResult getLayout(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return delegate.getLayout(session, tableHandle, constraint, desiredColumns);
    }

    @Override
    public TableLayout getLayout(Session session, TableHandle handle)
    {
        return delegate.getLayout(session, handle);
    }

    @Override
    public TableHandle getAlternativeTableHandle(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        return delegate.getAlternativeTableHandle(session, tableHandle, partitioningHandle);
    }

    @Override
    public boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle)
    {
        return delegate.isLegacyGetLayoutSupported(session, tableHandle);
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        return delegate.getCommonPartitioning(session, left, right);
    }

    @Override
    public boolean isRefinedPartitioningOver(Session session, PartitioningHandle a, PartitioningHandle b)
    {
        return delegate.isRefinedPartitioningOver(session, a, b);
    }

    @Override
    public PartitioningHandle getPartitioningHandleForExchange(Session session, String catalogName, int partitionCount, List<Type> partitionTypes)
    {
        return delegate.getPartitioningHandleForExchange(session, catalogName, partitionCount, partitionTypes);
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        return delegate.getInfo(session, handle);
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        return delegate.getTableMetadata(session, tableHandle);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        return delegate.getTableStatistics(session, tableHandle, columnHandles, constraint);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTables(session, prefix);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        return delegate.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        return delegate.toExplainIOConstraints(session, tableHandle, constraints);
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTableColumns(session, prefix);
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        delegate.createSchema(session, schema, properties);
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        delegate.dropSchema(session, schema);
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        delegate.renameSchema(session, source, target);
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        delegate.createTable(session, catalogName, tableMetadata, ignoreExisting);
    }

    @Override
    public TableHandle createTemporaryTable(Session session, String catalogName, List<ColumnMetadata> columns, Optional<PartitioningMetadata> partitioningMetadata)
    {
        return delegate.createTemporaryTable(session, catalogName, columns, partitioningMetadata);
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        delegate.renameTable(session, tableHandle, newTableName);
    }

    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Object> properties)
    {
        delegate.setTableProperties(session, tableHandle, properties);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        delegate.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        delegate.addColumn(session, tableHandle, column);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        delegate.dropColumn(session, tableHandle, column);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        delegate.dropTable(session, tableHandle);
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        delegate.truncateTable(session, tableHandle);
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getNewTableLayout(session, catalogName, tableMetadata);
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        return delegate.beginCreateTable(session, catalogName, tableMetadata, layout);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            Session session,
            OutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        return delegate.getInsertLayout(session, target);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getStatisticsCollectionMetadata(session, catalogName, tableMetadata);
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        return delegate.beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
        delegate.beginQuery(session, connectors);
    }

    @Override
    public void cleanupQuery(Session session)
    {
        delegate.cleanupQuery(session);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        return delegate.beginInsert(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            Session session,
            InsertTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishInsert(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ColumnHandle> getDeleteRowIdColumn(Session session, TableHandle tableHandle)
    {
        return delegate.getDeleteRowIdColumn(session, tableHandle);
    }

    @Override
    public Optional<ColumnHandle> getUpdateRowIdColumn(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return delegate.getUpdateRowIdColumn(session, tableHandle, updatedColumns);
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        return delegate.supportsMetadataDelete(session, tableHandle);
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle)
    {
        return delegate.metadataDelete(session, tableHandle);
    }

    @Override
    public DeleteTableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        return delegate.beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        delegate.finishDelete(session, tableHandle, fragments);
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return delegate.beginUpdate(session, tableHandle, updatedColumns);
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        delegate.finishUpdate(session, tableHandle, fragments);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        return delegate.getRowChangeParadigm(session, tableHandle);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        return delegate.getMergeRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public Optional<PartitioningHandle> getMergeUpdateLayout(Session session, TableHandle tableHandle)
    {
        return delegate.getMergeUpdateLayout(session, tableHandle);
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle)
    {
        return delegate.beginMerge(session, tableHandle);
    }

    @Override
    public void finishMerge(Session session, ExecutionWriterTarget.MergeHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishMerge(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        return delegate.getCatalogHandle(session, catalogName);
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        return delegate.getCatalogNames(session);
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listViews(session, prefix);
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.getViews(session, prefix);
    }

    @Override
    public void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        delegate.createView(session, catalogName, viewMetadata, viewData, replace);
    }

    @Override
    public void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        delegate.renameView(session, existingViewName, newViewName);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropView(session, viewName);
    }

    @Override
    public void createMaterializedView(
            Session session,
            String catalogName,
            ConnectorTableMetadata viewMetadata,
            MaterializedViewDefinition viewDefinition,
            boolean ignoreExisting)
    {
        delegate.createMaterializedView(session, catalogName, viewMetadata, viewDefinition, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle)
    {
        return delegate.beginRefreshMaterializedView(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            InsertTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishRefreshMaterializedView(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        return delegate.getReferencedMaterializedViews(session, tableName);
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session,
            TableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
    }

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        delegate.createRole(session, role, grantor, catalog);
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        delegate.dropRole(session, role, catalog);
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        return delegate.listRoles(session, catalog);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(session, catalog, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        delegate.grantRoles(session, roles, grantees, withAdminOption, grantor, catalog);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        delegate.revokeRoles(session, roles, grantees, adminOptionFor, grantor, catalog);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        return delegate.listApplicableRoles(session, principal, catalog);
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        return delegate.listEnabledRoles(session, catalog);
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTablePrivileges(session, prefix);
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return delegate.commitPageSinkAsync(session, tableHandle, fragments);
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        return delegate.commitPageSinkAsync(session, tableHandle, fragments);
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        return delegate.commitPageSinkAsync(session, tableHandle, fragments);
    }

    @Override
    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return delegate.getFunctionAndTypeManager();
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return delegate.getProcedureRegistry();
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return delegate.getBlockEncodingSerde();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return delegate.getSessionPropertyManager();
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return delegate.getSchemaPropertyManager();
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return delegate.getTablePropertyManager();
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        return delegate.getColumnPropertyManager();
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return delegate.getAnalyzePropertyManager();
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
    {
        return delegate.getConnectorCapabilities(session, catalogName);
    }

    @Override
    public void dropConstraint(Session session, TableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        delegate.dropConstraint(session, tableHandle, constraintName, columnName);
    }

    @Override
    public void addConstraint(Session session, TableHandle tableHandle, TableConstraint<String> tableConstraint)
    {
        delegate.addConstraint(session, tableHandle, tableConstraint);
    }

    @Override
    public String normalizeIdentifier(Session session, String catalogName, String identifier)
    {
        return delegate.normalizeIdentifier(session, catalogName, identifier);
    }
}
