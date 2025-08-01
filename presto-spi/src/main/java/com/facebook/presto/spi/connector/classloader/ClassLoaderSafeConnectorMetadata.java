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
package com.facebook.presto.spi.connector.classloader;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorMetadata
        implements ConnectorMetadata
{
    private final ConnectorMetadata delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorMetadata(ConnectorMetadata delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableLayouts(session, table, constraint, desiredColumns);
        }
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableLayoutForConstraint(session, table, constraint, desiredColumns);
        }
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableLayout(session, handle);
        }
    }

    @Override
    public boolean isLegacyGetLayoutSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isLegacyGetLayoutSupported(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCommonPartitioningHandle(session, left, right);
        }
    }

    @Override
    public boolean isRefinedPartitioningOver(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isRefinedPartitioningOver(session, left, right);
        }
    }

    @Override
    public ConnectorPartitioningHandle getPartitioningHandleForExchange(ConnectorSession session, int partitionCount, List<Type> partitionTypes)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitioningHandleForExchange(session, partitionCount, partitionTypes);
        }
    }

    @Override
    public ConnectorTableLayoutHandle getAlternativeLayoutHandle(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAlternativeLayoutHandle(session, tableLayoutHandle, partitioningHandle);
        }
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getNewTableLayout(session, tableMetadata);
        }
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getInsertLayout(session, tableHandle);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getStatisticsCollectionMetadata(session, tableMetadata);
        }
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginStatisticsCollection(session, tableHandle);
        }
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
        }
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.schemaExists(session, schemaName);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listSchemaNames(session);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemaProperties(session, schemaName);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandle(session, tableName);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandle(session, tableName, tableVersion);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties);
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSystemTable(session, tableName);
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableMetadata(session, table);
        }
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle table)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getInfo(table);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTables(session, schemaName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTables(session, schemaNameOrNull);
        }
    }

    @Override
    public Optional<List<SchemaTableName>> getReferencedMaterializedViews(ConnectorSession session, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getReferencedMaterializedViews(session, tableName);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandles(session, tableHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnMetadata(session, tableHandle, columnHandle);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(ConnectorSession session, ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.toExplainIOConstraints(session, tableHandle, constraints);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTableColumns(session, prefix);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableStatistics(session, tableHandle, tableLayoutHandle, columnHandles, constraint);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.addColumn(session, tableHandle, column);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.createSchema(session, schemaName, properties);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropSchema(session, schemaName);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.renameSchema(session, source, target);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.createTable(session, tableMetadata, ignoreExisting);
        }
    }

    @Override
    public ConnectorTableHandle createTemporaryTable(ConnectorSession session, List<ColumnMetadata> columns, Optional<ConnectorPartitioningMetadata> partitioningMetadata)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.createTemporaryTable(session, columns, partitioningMetadata);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropTable(session, tableHandle);
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.truncateTable(session, tableHandle);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.renameColumn(session, tableHandle, source, target);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropColumn(session, tableHandle, column);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.renameTable(session, tableHandle, newTableName);
        }
    }

    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> properties)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.setTableProperties(session, tableHandle, properties);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginCreateTable(session, tableMetadata, layout);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
        }
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.beginQuery(session);
        }
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.cleanupQuery(session);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginInsert(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finishInsert(session, insertHandle, fragments, computedStatistics);
        }
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.createView(session, viewMetadata, viewData, replace);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.renameView(session, viewName, newViewName);
        }
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getDeleteRowIdColumnHandle(session, tableHandle);
        }
    }

    @Override
    public Optional<ColumnHandle> getDeleteRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getDeleteRowIdColumn(session, tableHandle);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropView(session, viewName);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listViews(session, schemaName);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listViews(session, schemaNameOrNull);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getViews(session, prefix);
        }
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMaterializedView(session, viewName);
        }
    }

    @Override
    public void createMaterializedView(ConnectorSession session, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.createMaterializedView(session, viewMetadata, viewDefinition, ignoreExisting);
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropMaterializedView(session, viewName);
        }
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(ConnectorSession session, SchemaTableName materializedViewName)
    {
        return delegate.getMaterializedViewStatus(session, materializedViewName);
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(ConnectorSession session, SchemaTableName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMaterializedViewStatus(session, materializedViewName, baseQueryDomain);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginRefreshMaterializedView(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finishRefreshMaterializedView(session, insertHandle, fragments, computedStatistics);
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns);
        }
    }

    @Override
    public Optional<ColumnHandle> getUpdateRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getUpdateRowIdColumn(session, tableHandle, updatedColumns);
        }
    }

    @Override
    public ConnectorDeleteTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginDelete(session, tableHandle);
        }
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorDeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.finishDelete(session, tableHandle, fragments);
        }
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginUpdate(session, tableHandle, updatedColumns);
        }
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.finishUpdate(session, tableHandle, fragments);
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getRowChangeParadigm(session, tableHandle);
        }
    }

    /**
     * Get the column handle that will generate row IDs for the merge operation.
     * These IDs will be passed to the {@link com.facebook.presto.spi.ConnectorMergeSink#storeMergedRows}
     * method of the {@link com.facebook.presto.spi.ConnectorMergeSink} that created them.
     */
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMergeRowIdColumnHandle(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getMergeUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getMergeUpdateLayout(session, tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.beginMerge(session, tableHandle);
        }
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.finishMerge(session, mergeTableHandle, fragments, computedStatistics);
        }
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.supportsMetadataDelete(session, tableHandle, tableLayoutHandle);
        }
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.metadataDelete(session, tableHandle, tableLayoutHandle);
        }
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
        }
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.createRole(session, role, grantor);
        }
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropRole(session, role);
        }
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listRoles(session);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listRoleGrants(session, principal);
        }
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.grantRoles(connectorSession, roles, grantees, withAdminOption, grantor);
        }
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.revokeRoles(connectorSession, roles, grantees, adminOptionFor, grantor);
        }
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listApplicableRoles(session, principal);
        }
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listEnabledRoles(session);
        }
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTablePrivileges(session, prefix);
        }
    }

    @Override
    public CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
    }

    @Override
    public CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
    }

    @Override
    public CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorDeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.commitPageSinkAsync(session, tableHandle, fragments);
        }
    }

    @Override
    public List<ConnectorMetadataUpdateHandle> getMetadataUpdateResults(List<ConnectorMetadataUpdateHandle> metadataUpdateRequests, QueryId queryId)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMetadataUpdateResults(metadataUpdateRequests, queryId);
        }
    }

    @Override
    public void doMetadataUpdateCleanup(QueryId queryId)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.doMetadataUpdateCleanup(queryId);
        }
    }

    @Override
    public TableLayoutFilterCoverage getTableLayoutFilterCoverage(ConnectorTableLayoutHandle tableHandle, Set<String> relevantPartitionColumns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableLayoutFilterCoverage(tableHandle, relevantPartitionColumns);
        }
    }

    @Override
    public void dropConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.dropConstraint(session, tableHandle, constraintName, columnName);
        }
    }

    @Override
    public void addConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, TableConstraint<String> tableConstraint)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.addConstraint(session, tableHandle, tableConstraint);
        }
    }

    @Override
    public boolean isPushdownSupportedForFilter(ConnectorSession session, ConnectorTableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isPushdownSupportedForFilter(session, tableHandle, filter, symbolToColumnHandleMap);
        }
    }

    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.normalizeIdentifier(session, identifier);
        }
    }
}
