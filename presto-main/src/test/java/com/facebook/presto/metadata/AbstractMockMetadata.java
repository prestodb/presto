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
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public abstract class AbstractMockMetadata
        implements Metadata
{
    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlFunction> listFunctions(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableLayoutResult getLayout(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableLayout getLayout(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle getAlternativeTableHandle(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle)
    {
        return true;
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRefinedPartitioningOver(Session session, PartitioningHandle a, PartitioningHandle b)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitioningHandle getPartitioningHandleForExchange(Session session, String catalogName, int partitionCount, List<Type> partitionTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle createTemporaryTable(Session session, String catalogName, List<ColumnMetadata> columns, Optional<PartitioningMetadata> partitioningMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getPreferredShuffleLayoutForNewTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getPreferredShuffleLayoutForInsert(Session session, TableHandle target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupQuery(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, ConnectorMaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(Session session, QualifiedObjectName materializedViewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetadataUpdates getMetadataUpdateResults(Session session, QueryManager queryManager, MetadataUpdates metadataUpdateRequests, QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
    {
        throw new UnsupportedOperationException();
    }
}
