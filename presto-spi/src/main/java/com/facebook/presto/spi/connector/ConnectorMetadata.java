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
package com.facebook.presto.spi.connector;

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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.api.Experimental;
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
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_APPLICABLE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

public interface ConnectorMetadata
{
    String MODIFYING_ROWS_MESSAGE = "This connector does not support modifying table rows";

    /**
     * Checks if a schema exists. The connector may have schemas that exist
     * but are not enumerable via {@link #listSchemaNames}.
     */
    default boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

    /**
     * Returns the schemas provided by this connector.
     */
    List<String> listSchemaNames(ConnectorSession session);

    /**
     * Gets the schema properties for the specified schema.
     */
    default Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support schema properties");
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    /**
     * Returns an error for connectors which do not support table version AS OF/BEFORE expression.
     */
    default ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support table version AS OF/BEFORE expression");
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     * The returned table handle can contain information in analyzeProperties.
     */
    default ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support analyze");
    }

    /**
     * Returns the system table for the specified table name, if one exists.
     * The system tables handled via this method differ from those returned by {@link Connector#getSystemTables()}.
     * The former mechanism allows dynamic resolution of system tables, while the latter is
     * based on static list of system tables built during startup.
     */
    default Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     *
     * @deprecated replaced by {@link ConnectorMetadata#getTableLayoutForConstraint(ConnectorSession, ConnectorTableHandle, Constraint, Optional)}
     */
    @Deprecated
    default List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        return emptyList();
    }

    /**
     * Return a table layout result that satisfy the given constraint.
     * <p>
     * Connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     */
    default ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        List<ConnectorTableLayoutResult> layouts = getTableLayouts(session, table, constraint, desiredColumns);
        if (layouts.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Connector hasn't implemented either getTableLayoutForConstraint() or getTableLayouts()");
        }
        else if (layouts.size() > 1) {
            throw new PrestoException(NOT_SUPPORTED, "Connector returned multiple layouts for table " + table);
        }
        return layouts.get(0);
    }

    ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle);

    /**
     * Return a table layout handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table layout handle.
     * The provided table layout handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table layout handle,
     * as promised by {@link #getCommonPartitioningHandle}.
     */
    default ConnectorTableLayoutHandle getAlternativeLayoutHandle(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getCommonPartitioningHandle() is implemented without getAlternativeLayout()");
    }

    /**
     * Experimental: if true, the engine will invoke getLayout otherwise, getLayout will not be called.
     */
    @Deprecated
    @Experimental
    default boolean isLegacyGetLayoutSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return true;
    }

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    @Deprecated
    default Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        if (left.equals(right)) {
            return Optional.of(left);
        }
        return Optional.empty();
    }

    /**
     * Partitioning <code>a = {a_1, ... a_n}</code> is considered as a refined partitioning over
     * partitioning <code>b = {b_1, ... b_m}</code> if:
     * <ul>
     * <li> n >= m </li>
     * <li> For every partition <code>b_i</code> in partitioning <code>b</code>,
     * the rows it contains is the same as union of a set of partitions <code>a_{i_1}, a_{i_2}, ... a_{i_k}</code>
     * in partitioning <code>a</code>, i.e.
     * <p>
     * <code>b_i = a_{i_1} + a_{i_2} +  ... + a_{i_k}</code>
     * <li> Connector can transparently convert partitioning <code>a</code> to partitioning <code>b</code>
     * associated with the provided table layout handle.
     * </ul>
     *
     * <p>
     * For example, consider two partitioning over <code>order</code> table:
     * <ul>
     * <li> partitioning <code>a</code> has 256 partitions by <code>orderkey % 256</code>
     * <li> partitioning <code>b</code> has 128 partitions by <code>orderkey % 128</code>
     * </ul>
     *
     * <p>
     * Partitioning <code>a</code> is a refined partitioning over <code>b</code> if Connector supports
     * transparently convert <code>a</code> to <code>b</code>.
     * <p>
     * Refined-over relation is reflexive.
     * <p>
     * This SPI is unstable and subject to change in the future.
     */
    default boolean isRefinedPartitioningOver(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        return left.equals(right);
    }

    /**
     * Provides partitioning handle for exchange.
     */
    default ConnectorPartitioningHandle getPartitioningHandleForExchange(ConnectorSession session, int partitionCount, List<Type> partitionTypes)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support custom partitioning");
    }
    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);

    /**
     * Return the connector-specific metadata for the specified table layout. This is the object that is passed to the event listener framework.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    default Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        return Optional.empty();
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @deprecated replaced by {@link ConnectorMetadata#listTables(ConnectorSession, Optional)}
     */
    @Deprecated
    default List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     */
    default List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return listTables(session, schemaName.orElse(null));
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Returns a TupleDomain of constraints that is suitable for ExplainIO
     */
    default TupleDomain<ColumnHandle> toExplainIOConstraints(ConnectorSession session, ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        return constraints;
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix);

    /**
     * Get statistics for table for given columns and filtering constraint.
     */
    default TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        return TableStatistics.empty();
    }

    /**
     * Creates a schema.
     */
    default void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    /**
     * Drops the specified schema.
     *
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    default void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    /**
     * Renames the specified schema.
     */
    default void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws PrestoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    default void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    /**
     * Creates a temporary table with optional partitioning requirements.
     * Temporary table might have different default storage format, compression scheme, replication factor, etc,
     * and gets automatically dropped when the transaction ends.
     * <p>
     * This SPI is unstable and subject to change in the future.
     */
    default ConnectorTableHandle createTemporaryTable(ConnectorSession session, List<ColumnMetadata> columns, Optional<ConnectorPartitioningMetadata> partitioningMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating temporary tables");
    }

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    default void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    /**
     * Truncates the specified table
     *
     * @throws RuntimeException if the table cannot be truncated or table handle is no longer valid
     */
    default void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    /**
     * Rename the specified table
     */
    default void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    default void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> properties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setting table properties");
    }

    /**
     * Add the specified column
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    /**
     * Rename the specified column
     */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    /**
     * Drop the specified column
     */
    default void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    /**
     * Get the physical layout for a new table.
     */
    default Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    default Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableLayout layout = getTableLayoutForConstraint(session, tableHandle, new Constraint<>(TupleDomain.all(), map -> true), Optional.empty())
                .getTableLayout();

        if (!layout.getTablePartitioning().isPresent()) {
            return Optional.empty();
        }

        ConnectorPartitioningHandle partitioningHandle = layout.getTablePartitioning().get().getPartitioningHandle();
        Map<ColumnHandle, String> columnNamesByHandle = getColumnHandles(session, tableHandle).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        List<String> partitionColumns = layout.getTablePartitioning().get().getPartitioningColumns().stream()
                .map(columnNamesByHandle::get)
                .collect(toList());

        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    /**
     * Describes statistics that must be collected during a write.
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return TableStatisticsMetadata.empty();
    }

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandleForStatisticsCollection() is implemented without getStatisticsCollectionMetadata()");
    }

    /**
     * Begin statistics collection
     */
    default ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getStatisticsCollectionMetadata() is implemented without beginStatisticsCollection()");
    }

    /**
     * Finish statistics collection
     */
    default void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginStatisticsCollection() is implemented without finishStatisticsCollection()");
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    default ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    default Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginCreateTable() is implemented without finishCreateTable()");
    }

    /**
     * Start a SELECT/UPDATE/INSERT/DELETE query. This notification is triggered after the planning phase completes.
     */
    default void beginQuery(ConnectorSession session) {}

    /**
     * Cleanup after a SELECT/UPDATE/INSERT/DELETE query. This is the very last notification after the query finishes, whether it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    default void cleanupQuery(ConnectorSession session) {}

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    /**
     * Finish insert query
     */
    default Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginInsert() is implemented without finishInsert()");
    }

    /**
     * @deprecated Replaced by {@link #getDeleteRowIdColumn(ConnectorSession, ConnectorTableHandle)}
     */
    @Deprecated
    default ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Get the column handle that will generate row IDs for the delete operation, if this connector requires row IDs to support delete.
     * These IDs will be passed to the {@code deleteRows()} method of the
     * {@link com.facebook.presto.spi.UpdatablePageSource} that created them.  If the connector does not require row IDs to perform deletes,
     * then {@code Optional.empty()} may be returned.
     */
    default Optional<ColumnHandle> getDeleteRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.ofNullable(getDeleteRowIdColumnHandle(session, tableHandle));
    }

    /**
     * @deprecated Replaced by {@link #getUpdateRowIdColumn(ConnectorSession, ConnectorTableHandle, List)}
     */
    @Deprecated
    default ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    /**
     * Get the column handle that will generate row IDs for the update operation, if this connector requires rowIDs to support update. If the connector
     * does not require row IDs to perform updates, then {@code Optional.empty()} may be returned.
     */
    default Optional<ColumnHandle> getUpdateRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return Optional.ofNullable(getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns));
    }

    /**
     * Get the column handle that will generate row IDs for the merge operation.
     * These IDs will be passed to the {@link com.facebook.presto.spi.ConnectorMergeSink#storeMergedRows}
     * method of the {@link com.facebook.presto.spi.ConnectorMergeSink} that created them.
     */
    default ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    /**
     * Begin delete query
     */
    default ConnectorDeleteTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Finish delete query
     *
     * @param fragments all fragments returned by {@link com.facebook.presto.spi.UpdatablePageSource#finish()}
     */
    default void finishDelete(ConnectorSession session, ConnectorDeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    default ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support update");
    }

    default void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support update");
    }

    /**
     * Return the row change paradigm supported by the connector on the table.
     */
    default RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    /**
     * Get the physical layout for updated rows of a MERGE operation.
     * Inserted rows are handled by {@link #getInsertLayout}.
     * This layout always uses the {@link #getMergeRowIdColumnHandle merge row ID column}.
     */
    default Optional<ConnectorPartitioningHandle> getMergeUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.empty();
    }

    /**
     * Do whatever is necessary to start an MERGE query, returning the {@link ConnectorMergeTableHandle}
     * instance that will be passed to the PageSink, and to the {@link #finishMerge} method.
     */
    default ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    /**
     * Finish a merge query
     *
     * @param session The session
     * @param mergeTableHandle A ConnectorMergeTableHandle for the table that is the target of the merge
     * @param fragments All fragments returned by the merge plan
     * @param computedStatistics Statistics for the table, meaningful only to the connector that produced them.
     */
    default void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginMerge() is implemented without finishMerge()");
    }

    /**
     * Create the specified view. The data for the view is opaque to the connector.
     */
    default void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    /**
     * Rename the specified view
     */
    default void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming views");
    }

    /**
     * Drop the specified view.
     */
    default void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }

    /**
     * List view names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @deprecated replaced by {@link ConnectorMetadata#listViews(ConnectorSession, Optional)}
     */
    @Deprecated
    default List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    default List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return listViews(session, schemaName.orElse(null));
    }

    /**
     * Gets the view data for views that match the specified table prefix.
     */
    default Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    /**
     * Gets the materialized view data for the specified materialized view name.
     */
    default Optional<MaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    /**
     * Create the specified materialized view. The data for the materialized view is opaque to the connector.
     */
    default void createMaterializedView(ConnectorSession session, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating materialized views");
    }

    /**
     * Drop the specified materialized view.
     */
    default void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping materialized views");
    }

    /**
     * Get the materialized view status to inform the engine how much data has been materialized in the view
     * @param baseQueryDomain The domain from which to consider missing partitions. For example, a query that
     * selects a specific date range can consider only partitions from that range when determining view staleness.
     */
    default MaterializedViewStatus getMaterializedViewStatus(ConnectorSession session, SchemaTableName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support getting materialized views status");
    }

    default MaterializedViewStatus getMaterializedViewStatus(ConnectorSession session, SchemaTableName materializedViewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support getting materialized views status");
    }

    /**
     * Begin refresh materialized view
     */
    default ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support refresh materialized views");
    }

    /**
     * Finish refresh materialized view
     */
    default Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata finishRefreshMaterializedView() is not implemented");
    }

    /**
     * Gets the referenced materialized views for a give table
     */
    default Optional<List<SchemaTableName>> getReferencedMaterializedViews(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    /**
     * @return whether delete without table scan is supported
     */
    default boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Delete the provided table layout
     *
     * @return number of rows deleted, or null for unknown
     */
    default OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    default Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return Optional.empty();
    }

    /**
     * Creates the specified role.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    default void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support create role");
    }

    /**
     * Drops the specified role.
     */
    default void dropRole(ConnectorSession session, String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support drop role");
    }

    /**
     * List available roles.
     */
    default Set<String> listRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List role grants for a given principal, not recursively.
     */
    default Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified roles to the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Revokes the specified roles from the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    default Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    default Set<String> listEnabledRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    default void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support grants");
    }

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    default void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support revokes");
    }

    /**
     * List the table privileges granted to the specified grantee for the tables that have the specified prefix considering the selected session role
     */
    default List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyList();
    }

    /**
     * Commits page sink for table creation.
     * To enable recoverable grouped execution, it is required that output connector supports page sink commit.
     * This method is unstable and subject to change in the future.
     */
    @Experimental
    default CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support page sink commit");
    }

    /**
     * Commits page sink for table insertion.
     * To enable recoverable grouped execution, it is required that output connector supports page sink commit.
     * This method is unstable and subject to change in the future.
     */
    @Experimental
    default CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support page sink commit");
    }

    /**
     * Commits page sink for table insertion.
     * To enable recoverable grouped execution, it is required that output connector supports page sink commit.
     * This method is unstable and subject to change in the future.
     */
    @Experimental
    default CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorDeleteTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support page sink commit");
    }

    /**
     * Handles metadata update requests and sends the results back to worker
     */
    default List<ConnectorMetadataUpdateHandle> getMetadataUpdateResults(List<ConnectorMetadataUpdateHandle> metadataUpdateRequests, QueryId queryId)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support metadata update requests");
    }

    default void doMetadataUpdateCleanup(QueryId queryId)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support metadata update cleanup");
    }

    default TableLayoutFilterCoverage getTableLayoutFilterCoverage(ConnectorTableLayoutHandle tableHandle, Set<String> relevantPartitionColumns)
    {
        return NOT_APPLICABLE;
    }

    /**
     * Drop the specified constraint
     */
    default void dropConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping table constraints");
    }

    /**
     * Add the specified constraint
     */
    default void addConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, TableConstraint<String> constraint)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding table constraints");
    }

    /**
     * Check if pushdown is supported for the given filter against columns of the table handle
     *
     * @param session
     * @param tableHandle
     * @param filter
     * @param symbolToColumnHandleMap
     * @return
     */
    default boolean isPushdownSupportedForFilter(ConnectorSession session, ConnectorTableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
    {
        return false;
    }

    /**
     * Normalize the provided SQL identifier according to connector-specific rules
     */
    default String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return identifier.toLowerCase(ENGLISH);
    }
}
