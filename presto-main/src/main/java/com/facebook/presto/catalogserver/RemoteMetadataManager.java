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
package com.facebook.presto.catalogserver;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.AnalyzeTableHandle;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.PartitioningMetadata;
import com.facebook.presto.metadata.ProcedureRegistry;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
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
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RemoteMetadataManager
        implements Metadata
{
    private static final String EMPTY_STRING = "";
    private final Metadata delegate;
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final DriftClient<CatalogServerClient> catalogServerClient;

    @Inject
    public RemoteMetadataManager(
            MetadataManager metadataManager,
            TransactionManager transactionManager,
            ObjectMapper objectMapper,
            DriftClient<CatalogServerClient> catalogServerClient)
    {
        this.delegate = requireNonNull(metadataManager, "metadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.catalogServerClient = requireNonNull(catalogServerClient, "catalogServerClient is null");
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
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        return catalogServerClient.get().schemaExists(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                schema);
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        return catalogServerClient.get().catalogExists(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                catalogName);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        String schemaNamesJson = catalogServerClient.get().listSchemaNames(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                catalogName);
        if (!schemaNamesJson.equals(EMPTY_STRING)) {
            try {
                List<String> schemaNames;
                schemaNames = objectMapper.readValue(schemaNamesJson, new TypeReference<List<String>>() {});
                return schemaNames;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        String tableHandleJson = catalogServerClient.get().getTableHandle(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                table);
        if (!tableHandleJson.equals(EMPTY_STRING)) {
            try {
                TableHandle tableHandle = objectMapper.readValue(tableHandleJson, TableHandle.class);
                Optional<CatalogMetadata> catalogMetadata = this.transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), table.getCatalogName());
                if (catalogMetadata.isPresent()) {
                    ConnectorTransactionHandle connectorTransactionHandle = catalogMetadata.get().getTransactionHandleFor(tableHandle.getConnectorId());
                    tableHandle.setTransaction(connectorTransactionHandle);
                }
                return Optional.of(tableHandle);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName table, Map<String, Object> analyzeProperties)
    {
        return delegate.getTableHandleForStatisticsCollection(session, table, analyzeProperties);
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public TableLayoutResult getLayout(Session session, TableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return delegate.getLayout(session, table, constraint, desiredColumns);
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
    public boolean isRefinedPartitioningOver(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        return delegate.isRefinedPartitioningOver(session, left, right);
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
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        String tableListJson = catalogServerClient.get().listTables(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        if (!tableListJson.equals(EMPTY_STRING)) {
            try {
                List<QualifiedObjectName> tableList;
                tableList = objectMapper.readValue(tableListJson, new TypeReference<List<QualifiedObjectName>>() {});
                return tableList;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<>();
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
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        return delegate.getInsertLayout(session, table);
    }

    @Override
    public Optional<NewTableLayout> getPreferredShuffleLayoutForInsert(Session session, TableHandle table)
    {
        return delegate.getPreferredShuffleLayoutForInsert(session, table);
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
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getNewTableLayout(session, catalogName, tableMetadata);
    }

    @Override
    public Optional<NewTableLayout> getPreferredShuffleLayoutForNewTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getPreferredShuffleLayoutForNewTable(session, catalogName, tableMetadata);
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
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        return delegate.beginCreateTable(session, catalogName, tableMetadata, layout);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        return delegate.beginInsert(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishInsert(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        return delegate.getUpdateRowIdColumnHandle(session, tableHandle);
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
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        return delegate.beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        delegate.finishDelete(session, tableHandle, fragments);
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
        String viewsListJson = catalogServerClient.get().listViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        if (!viewsListJson.equals(EMPTY_STRING)) {
            try {
                List<QualifiedObjectName> viewsList;
                viewsList = objectMapper.readValue(viewsListJson, new TypeReference<List<QualifiedObjectName>>() {});
                return viewsList;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        String viewsMapJson = catalogServerClient.get().getViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        if (!viewsMapJson.equals(EMPTY_STRING)) {
            try {
                Map<QualifiedObjectName, ViewDefinition> viewsMap;
                viewsMap = objectMapper.readValue(viewsMapJson, new TypeReference<Map<QualifiedObjectName, ViewDefinition>>() {});
                return viewsMap;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new HashMap<>();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        String viewDefinitionJson = catalogServerClient.get().getView(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                viewName);
        if (!viewDefinitionJson.equals(EMPTY_STRING)) {
            try {
                ViewDefinition viewDefinition = objectMapper.readValue(viewDefinitionJson, ViewDefinition.class);
                return Optional.of(viewDefinition);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        delegate.createView(session, catalogName, viewMetadata, viewData, replace);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropView(session, viewName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        String connectorMaterializedViewDefinitionJson = catalogServerClient.get().getMaterializedView(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                viewName);
        if (!connectorMaterializedViewDefinitionJson.equals(EMPTY_STRING)) {
            try {
                ConnectorMaterializedViewDefinition connectorMaterializedViewDefinition = objectMapper.readValue(connectorMaterializedViewDefinitionJson, ConnectorMaterializedViewDefinition.class);
                return Optional.of(connectorMaterializedViewDefinition);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean isMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return delegate.isMaterializedView(session, viewName);
    }

    @Override
    public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, ConnectorMaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        delegate.createMaterializedView(session, catalogName, viewMetadata, viewDefinition, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(Session session, QualifiedObjectName materializedViewName)
    {
        return delegate.getMaterializedViewStatus(session, materializedViewName);
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle)
    {
        return delegate.beginRefreshMaterializedView(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishRefreshMaterializedView(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        String referencedMaterializedViewsListJson = catalogServerClient.get().getReferencedMaterializedViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                tableName);
        if (!referencedMaterializedViewsListJson.equals(EMPTY_STRING)) {
            try {
                List<QualifiedObjectName> referencedMaterializedViewsList;
                referencedMaterializedViewsList = objectMapper.readValue(referencedMaterializedViewsListJson, new TypeReference<List<QualifiedObjectName>>() {});
                return referencedMaterializedViewsList;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
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
    public MetadataUpdates getMetadataUpdateResults(Session session, QueryManager queryManager, MetadataUpdates metadataUpdateRequests, QueryId queryId)
    {
        return delegate.getMetadataUpdateResults(session, queryManager, metadataUpdateRequests, queryId);
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
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId connectorId)
    {
        return delegate.getConnectorCapabilities(session, connectorId);
    }

    @Override
    public TableLayoutFilterCoverage getTableLayoutFilterCoverage(Session session, TableHandle tableHandle, Set<String> relevantPartitionColumns)
    {
        return delegate.getTableLayoutFilterCoverage(session, tableHandle, relevantPartitionColumns);
    }
}
