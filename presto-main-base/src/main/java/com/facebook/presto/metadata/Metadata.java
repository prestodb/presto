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
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MergeHandle;
import com.facebook.presto.spi.NewTableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.plan.PartitioningHandle;
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

import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_APPLICABLE;

public interface Metadata
{
    void verifyComparableOrderableContract();

    Type getType(TypeSignature signature);

    void registerBuiltInFunctions(List<? extends SqlFunction> functions);

    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Gets the schema properties for the specified schema.
     */
    Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName);

    Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName);

    /**
     * Returns a table handle for time travel expression
     */
    Optional<TableHandle> getHandleVersion(Session session, QualifiedObjectName tableName, Optional<ConnectorTableVersion> tableVersion);

    Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties);

    /**
     * Returns a new table layout that satisfies the given constraint together with unenforced constraint.
     */
    @Experimental
    TableLayoutResult getLayout(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns);

    /**
     * Returns table's layout properties for a given table handle.
     */
    @Experimental
    TableLayout getLayout(Session session, TableHandle handle);

    /**
     * Return a table handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table layout handle.
     * The provided table layout handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table layout handle,
     * as promised by {@link #getCommonPartitioning}.
     */
    TableHandle getAlternativeTableHandle(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle);

    /**
     * Experimental: if true, the engine will invoke getLayout otherwise, getLayout will not be called.
     * If filter pushdown is required, use a ConnectorPlanOptimizer in the respective connector in order
     * to push compute into it's TableScan.
     */
    @Deprecated
    boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle);

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    @Deprecated
    Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right);

    /**
     * Return whether {@code left} is a refined partitioning over {@code right}.
     * See
     * {@link com.facebook.presto.spi.connector.ConnectorMetadata#isRefinedPartitioningOver(ConnectorSession, ConnectorPartitioningHandle, ConnectorPartitioningHandle)}
     * for details about refined partitioning.
     * <p>
     * Refined-over relation is reflexive.
     */
    @Experimental
    boolean isRefinedPartitioningOver(Session session, PartitioningHandle a, PartitioningHandle b);

    /**
     * Provides partitioning handle for exchange.
     */
    PartitioningHandle getPartitioningHandleForExchange(Session session, String catalogName, int partitionCount, List<Type> partitionTypes);

    Optional<Object> getInfo(Session session, TableHandle handle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Return statistics for specified table for given columns and filtering constraint.
     */
    TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Returns a TupleDomain of constraints that is suitable for ExplainIO
     */
    TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates a schema.
     */
    void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties);

    /**
     * Drops the specified schema.
     */
    void dropSchema(Session session, CatalogSchemaName schema);

    /**
     * Renames the specified schema.
     */
    void renameSchema(Session session, CatalogSchemaName source, String target);

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws PrestoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting);

    /**
     * Creates a temporary table with optional partitioning requirements.
     * Temporary table might have different default storage format, compression scheme, replication factor, etc,
     * and gets automatically dropped when the transaction ends.
     */
    @Experimental
    TableHandle createTemporaryTable(Session session, String catalogName, List<ColumnMetadata> columns, Optional<PartitioningMetadata> partitioningMetadata);

    /**
     * Rename the specified table.
     */
    void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName);

    /**
     * Set properties to the specified table.
     */
    void setTableProperties(Session session, TableHandle tableHandle, Map<String, Object> properties);

    /**
     * Rename the specified column.
     */
    void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target);

    /**
     * Add the specified column to the table.
     */
    void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column);

    /**
     * Drop the specified column.
     */
    void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(Session session, TableHandle tableHandle);

    /**
     * Truncates the specified table
     */
    void truncateTable(Session session, TableHandle tableHandle);

    Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout);

    /**
     * Finish a table creation with data after the data is written.
     */
    Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target);

    /**
     * Describes statistics that must be collected during a write.
     */
    TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin statistics collection
     */
    AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle);

    /**
     * Finish statistics collection
     */
    void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics);

    /**
     * Start a SELECT/UPDATE/INSERT/DELETE query
     */
    void beginQuery(Session session, Set<ConnectorId> connectors);

    /**
     * Cleanup after a query. This is the very last notification after the query finishes, regardless if it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    void cleanupQuery(Session session);

    /**
     * Begin insert query
     */
    InsertTableHandle beginInsert(Session session, TableHandle tableHandle);

    /**
     * Finish insert query
     */
    Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Get the row ID column handle used with UpdatablePageSource#deleteRows.
     */
    Optional<ColumnHandle> getDeleteRowIdColumn(Session session, TableHandle tableHandle);

    /**
     * Get the row ID column handle used with UpdatablePageSource.
     */
    Optional<ColumnHandle> getUpdateRowIdColumn(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns);

    /**
     * @return whether delete without table scan is supported
     */
    boolean supportsMetadataDelete(Session session, TableHandle tableHandle);

    /**
     * Delete the provide table layout
     *
     * @return number of rows deleted, or empty for unknown
     */
    OptionalLong metadataDelete(Session session, TableHandle tableHandle);

    /**
     * Begin delete query
     */
    DeleteTableHandle beginDelete(Session session, TableHandle tableHandle);

    /**
     * Finish delete query
     */
    void finishDelete(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Begin update query
     */
    TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns);

    /**
     * Finish update query
     */
    void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Return the row update paradigm supported by the connector on the table or throw
     * an exception if row change is not supported.
     */
    RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle);

    /**
     * Get the column handle that will generate row IDs for the merge operation.
     * These IDs will be passed to the {@code storeMergedRows()} method of the
     * {@link com.facebook.presto.spi.ConnectorMergeSink} that created them.
     */
    ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle);

    /**
     * Get the physical layout for updated rows of a MERGE operation.
     */
    Optional<PartitioningHandle> getMergeUpdateLayout(Session session, TableHandle tableHandle);

    /**
     * Begin merge query
     */
    MergeHandle beginMerge(Session session, TableHandle tableHandle);

    /**
     * Finish merge query
     */
    void finishMerge(Session session, ExecutionWriterTarget.MergeHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Returns a connector id for the specified catalog name.
     */
    Optional<ConnectorId> getCatalogHandle(Session session, String catalogName);

    /**
     * Gets all the loaded catalogs
     *
     * @return Map of catalog name to connector id
     */
    Map<String, ConnectorId> getCatalogNames(Session session);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates the specified view with the specified view definition.
     */
    void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace);

    /**
     * Rename the specified view.
     */
    void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName);

    /**
     * Drops the specified view.
     */
    void dropView(Session session, QualifiedObjectName viewName);

    /**
     * Creates the specified materialized view with the specified view definition.
     */
    void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting);

    /**
     * Drops the specified materialized view.
     */
    void dropMaterializedView(Session session, QualifiedObjectName viewName);

    /**
     * Begin refresh materialized view
     */
    InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle);

    /**
     * Finish refresh materialized view
     */
    Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Gets the referenced materialized views for a give table
     */
    List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName);

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain);

    /**
     * Creates the specified role in the specified catalog.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * Drops the specified role in the specified catalog.
     */
    void dropRole(Session session, String role, String catalog);

    /**
     * List available roles in specified catalog.
     */
    Set<String> listRoles(Session session, String catalog);

    /**
     * List roles grants in the specified catalog for a given principal, not recursively.
     */
    Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal);

    /**
     * Grants the specified roles to the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * Revokes the specified roles from the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog);

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    Set<String> listEnabledRoles(Session session, String catalog);

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption);

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption);

    /**
     * Gets the privileges for the specified table available to the given grantee considering the selected session role
     */
    List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix);

    /**
     * Commits page sink for table creation.
     */
    @Experimental
    ListenableFuture<Void> commitPageSinkAsync(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Commits page sink for table insertion.
     */
    @Experimental
    ListenableFuture<Void> commitPageSinkAsync(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Commits page sink for table deletion.
     */
    @Experimental
    ListenableFuture<Void> commitPageSinkAsync(Session session, DeleteTableHandle tableHandle, Collection<Slice> fragments);

    MetadataUpdates getMetadataUpdateResults(Session session, QueryManager queryManager, MetadataUpdates metadataUpdates, QueryId queryId);

    // TODO: metadata should not provide FunctionAndTypeManager
    FunctionAndTypeManager getFunctionAndTypeManager();

    ProcedureRegistry getProcedureRegistry();

    BlockEncodingSerde getBlockEncodingSerde();

    SessionPropertyManager getSessionPropertyManager();

    SchemaPropertyManager getSchemaPropertyManager();

    TablePropertyManager getTablePropertyManager();

    ColumnPropertyManager getColumnPropertyManager();

    AnalyzePropertyManager getAnalyzePropertyManager();

    MetadataResolver getMetadataResolver(Session session);

    Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName);

    /**
     * Check if there is filter coverage of the specified partitioning keys
     *
     * @return NOT_APPLICABLE if partitioning is unsupported, not applicable, or the partitioning key is not relevant, NONE or COVERED otherwise
     */
    default TableLayoutFilterCoverage getTableLayoutFilterCoverage(Session session, TableHandle tableHandle, Set<String> relevantPartitionColumn)
    {
        return NOT_APPLICABLE;
    }

    void dropConstraint(Session session, TableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName);

    void addConstraint(Session session, TableHandle tableHandle, TableConstraint<String> tableConstraint);

    /**
     * Check if the join predicate can be translated and pushed down to the underlying datasource
     *
     * @return TRUE if there is connector is able to translate and push down join predicate to the underlying datasource, FALSE otherwise
     */
    default boolean isPushdownSupportedForFilter(Session session, TableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
    {
        return false;
    }

    String normalizeIdentifier(Session session, String catalogName, String identifier);
}
