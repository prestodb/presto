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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.BucketManager;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DistributionInfo;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNode;
import com.facebook.presto.raptorx.metadata.SchemaInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.storage.ChunkDelta;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorTablePartitioning;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.CreateDistributionProcedure.MAX_BUCKETS;
import static com.facebook.presto.raptorx.CreateDistributionProcedure.bucketNodes;
import static com.facebook.presto.raptorx.RaptorColumnHandle.BUCKET_NUMBER_HANDLE;
import static com.facebook.presto.raptorx.RaptorColumnHandle.CHUNK_ID_HANDLE;
import static com.facebook.presto.raptorx.RaptorColumnHandle.CHUNK_ROW_ID_HANDLE;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.RaptorTableProperties.BUCKETED_ON_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.COMPRESSION_TYPE_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.DISTRIBUTION_NAME_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.ORDERING_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static com.facebook.presto.raptorx.RaptorTableProperties.getBucketColumns;
import static com.facebook.presto.raptorx.RaptorTableProperties.getBucketCount;
import static com.facebook.presto.raptorx.RaptorTableProperties.getCompressionType;
import static com.facebook.presto.raptorx.RaptorTableProperties.getDistributionName;
import static com.facebook.presto.raptorx.RaptorTableProperties.getSortColumns;
import static com.facebook.presto.raptorx.RaptorTableProperties.getTemporalColumn;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private static final JsonCodec<ChunkInfo> CHUNK_INFO_CODEC = jsonCodec(ChunkInfo.class);
    private static final JsonCodec<ChunkDelta> CHUNK_DELTA_CODEC = jsonCodec(ChunkDelta.class);

    private final NodeSupplier nodeSupplier;
    private final BucketManager bucketManager;
    private final Transaction transaction;

    public RaptorMetadata(NodeSupplier nodeSupplier, BucketManager bucketManager, Transaction transaction)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.bucketManager = requireNonNull(bucketManager, "bucketManager is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return transaction.getSchemaId(schemaName).isPresent();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return transaction.listSchemas();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        transaction.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        transaction.dropSchema(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        transaction.renameSchema(source, target);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return transaction.getTableId(tableName)
                .map(tableId -> new RaptorTableHandle(tableId, OptionalLong.empty()))
                .orElse(null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;

        TableInfo tableInfo = transaction.getTableInfo(table.getTableId());
        long distributionId = tableInfo.getDistributionId();
        List<Long> bucketNodes = bucketManager.getBucketNodes(distributionId);

        List<ColumnHandle> bucketing = tableInfo.getBucketColumns().stream()
                .map(RaptorMetadata::toColumnHandle)
                .collect(toImmutableList());

        RaptorPartitioningHandle partitioning = new RaptorPartitioningHandle(distributionId, bucketNodes);
        RaptorTableLayoutHandle handle = new RaptorTableLayoutHandle(table, constraint.getSummary(), partitioning, bucketing);

        return ImmutableList.of(new ConnectorTableLayoutResult(createTableLayout(handle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        return createTableLayout((RaptorTableLayoutHandle) layoutHandle);
    }

    private static ConnectorTableLayout createTableLayout(RaptorTableLayoutHandle handle)
    {
        if (handle.getBucketColumns().isEmpty()) {
            return new ConnectorTableLayout(handle);
        }
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.of(new ConnectorTablePartitioning(
                        handle.getPartitioning(),
                        ImmutableList.copyOf(handle.getBucketColumns()))),
                Optional.of(ImmutableSet.copyOf(handle.getBucketColumns())),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata metadata)
    {
        long distributionId = getOrCreateDistribution(metadata.getColumns(), metadata.getProperties());

        List<Long> bucketNodes = bucketManager.getBucketNodes(distributionId);

        return Optional.of(new ConnectorNewTableLayout(
                new RaptorPartitioningHandle(distributionId, bucketNodes),
                getBucketColumns(metadata.getProperties())));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableInfo table = transaction.getTableInfo(((RaptorTableHandle) tableHandle).getTableId());
        SchemaInfo schema = transaction.getSchemaInfo(table.getSchemaId());
        DistributionInfo distribution = transaction.getDistributionInfo(table.getDistributionId());

        List<String> bucketing = table.getBucketColumns().stream()
                .map(ColumnInfo::getColumnName)
                .collect(toImmutableList());

        List<String> ordering = table.getSortColumns().stream()
                .map(ColumnInfo::getColumnName)
                .collect(toImmutableList());

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        properties.put(COMPRESSION_TYPE_PROPERTY, table.getCompressionType());

        table.getTemporalColumn().ifPresent(column -> properties.put(TEMPORAL_COLUMN_PROPERTY, column.getColumnName()));

        if (!bucketing.isEmpty()) {
            properties.put(BUCKETED_ON_PROPERTY, bucketing);
        }
        if (!ordering.isEmpty()) {
            properties.put(ORDERING_PROPERTY, ordering);
        }

        properties.put(BUCKET_COUNT_PROPERTY, distribution.getBucketCount());
        distribution.getDistributionName().ifPresent(name -> properties.put(DISTRIBUTION_NAME_PROPERTY, name));

        List<ColumnMetadata> columns = table.getColumns().stream()
                .map(RaptorMetadata::toColumnMetadata)
                .collect(toCollection(ArrayList::new));

        columns.add(toColumnMetadata(CHUNK_ID_HANDLE));

        if (!bucketing.isEmpty()) {
            columns.add(toColumnMetadata(BUCKET_NUMBER_HANDLE));
        }

        return new ConnectorTableMetadata(
                new SchemaTableName(schema.getSchemaName(), table.getTableName()),
                columns,
                properties.build(),
                table.getComment());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName.map(transaction::listTables)
                .orElseGet(transaction::listTables);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableInfo table = transaction.getTableInfo(((RaptorTableHandle) tableHandle).getTableId());

        List<RaptorColumnHandle> columns = table.getColumns().stream()
                .map(RaptorMetadata::toColumnHandle)
                .collect(toCollection(ArrayList::new));

        columns.add(CHUNK_ID_HANDLE);

        if (!table.getBucketColumns().isEmpty()) {
            columns.add(BUCKET_NUMBER_HANDLE);
        }

        return columns.stream().collect(toImmutableMap(RaptorColumnHandle::getColumnName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return toColumnMetadata((RaptorColumnHandle) columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        }

        // TODO: this should fetch all tables at once
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> map = ImmutableMap.builder();
        for (SchemaTableName tableName : tableNames) {
            transaction.getTableId(tableName)
                    .map(transaction::getTableInfo)
                    .ifPresent(table -> map.put(tableName, table.getColumns().stream()
                            .map(RaptorMetadata::toColumnMetadata)
                            .collect(toImmutableList())));
        }
        return map.build();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        transaction.dropTable(((RaptorTableHandle) tableHandle).getTableId());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!schemaExists(session, newTableName.getSchemaName())) {
            throw new SchemaNotFoundException(newTableName.getSchemaName());
        }
        transaction.renameTable(((RaptorTableHandle) tableHandle).getTableId(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        transaction.addColumn(
                ((RaptorTableHandle) tableHandle).getTableId(),
                column.getName(),
                column.getType(),
                Optional.ofNullable(column.getComment()));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        long tableId = ((RaptorTableHandle) tableHandle).getTableId();
        long columnId = ((RaptorColumnHandle) source).getColumnId();
        transaction.renameColumn(tableId, columnId, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        long tableId = ((RaptorTableHandle) tableHandle).getTableId();
        long columnId = ((RaptorColumnHandle) columnHandle).getColumnId();

        TableInfo table = transaction.getTableInfo(tableId);

        if (table.getTemporalColumnId().isPresent() && (table.getTemporalColumnId().getAsLong() == columnId)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the temporal column");
        }

        ColumnInfo column = table.getColumns().stream()
                .filter(info -> info.getColumnId() == columnId)
                .collect(onlyElement());

        if (column.getBucketOrdinal().isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop bucket columns");
        }
        if (column.getSortOrdinal().isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop sort columns");
        }

        transaction.dropColumn(tableId, columnId);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        RaptorPartitioningHandle partitioning = layout
                .map(ConnectorNewTableLayout::getPartitioning)
                .map(RaptorPartitioningHandle.class::cast)
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "New table layout is not set"));

        long schemaId = transaction.getSchemaId(tableMetadata.getTable().getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(tableMetadata.getTable().getSchemaName()));
        long tableId = transaction.nextTableId();

        List<RaptorColumnHandle> columnHandles = tableMetadata.getColumns().stream()
                .map(column -> new RaptorColumnHandle(
                        transaction.nextColumnId(),
                        column.getName(),
                        column.getType(),
                        Optional.ofNullable(column.getComment())))
                .collect(toImmutableList());

        Map<String, RaptorColumnHandle> columnHandleMap = uniqueIndex(columnHandles, RaptorColumnHandle::getColumnName);
        Map<String, Object> properties = tableMetadata.getProperties();

        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(properties), columnHandleMap);
        Optional<RaptorColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(properties), columnHandleMap);

        if (temporalColumnHandle.isPresent()) {
            RaptorColumnHandle column = temporalColumnHandle.get();
            if (!column.getColumnType().equals(DATE) && !column.getColumnType().equals(TIMESTAMP)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type date or timestamp: " + column.getColumnName());
            }
        }

        List<RaptorColumnHandle> bucketColumnHandles = getBucketColumns(properties).stream()
                .map(columnHandleMap::get)
                .collect(toImmutableList());

        transaction.registerTable(tableId);
        long transactionId = transaction.getTransactionId();

        return new RaptorOutputTableHandle(
                transactionId,
                schemaId,
                tableId,
                tableMetadata.getTable().getTableName(),
                tableMetadata.getComment(),
                columnHandles,
                sortColumnHandles,
                temporalColumnHandle,
                partitioning.getDistributionId(),
                partitioning.getBucketNodes().size(),
                bucketColumnHandles,
                getCompressionType(properties));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        RaptorOutputTableHandle table = (RaptorOutputTableHandle) tableHandle;

        List<ColumnInfo> columns = mapWithIndex(table.getColumnHandles().stream(), (column, index) ->
                new ColumnInfo(
                        column.getColumnId(),
                        column.getColumnName(),
                        column.getColumnType(),
                        column.getComment(),
                        toIntExact(index + 1),
                        listOrdinal(table.getBucketColumnHandles(), column),
                        listOrdinal(table.getSortColumnHandles(), column)))
                .collect(toImmutableList());

        transaction.createTable(
                table.getTableId(),
                table.getSchemaId(),
                table.getTableName(),
                table.getDistributionId(),
                table.getTemporalColumnHandle()
                        .map(RaptorColumnHandle::getColumnId)
                        .map(OptionalLong::of)
                        .orElse(OptionalLong.empty()),
                table.getCompressionType(),
                System.currentTimeMillis(),
                table.getComment(),
                columns);

        transaction.insertChunks(table.getTableId(), deserializeChunks(fragments));

        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableInfo table = transaction.getTableInfo(((RaptorTableHandle) tableHandle).getTableId());
        DistributionInfo distribution = transaction.getDistributionInfo(table.getDistributionId());

        List<RaptorColumnHandle> columnHandles = table.getColumns().stream()
                .map(RaptorMetadata::toColumnHandle)
                .collect(toImmutableList());

        transaction.registerTable(table.getTableId());
        long transactionId = transaction.getTransactionId();

        return new RaptorInsertTableHandle(
                transactionId,
                table.getTableId(),
                columnHandles,
                table.getSortColumns().stream()
                        .map(RaptorMetadata::toColumnHandle)
                        .collect(toImmutableList()),
                distribution.getBucketCount(),
                table.getBucketColumns().stream()
                        .map(RaptorMetadata::toColumnHandle)
                        .collect(toImmutableList()),
                table.getTemporalColumn().map(RaptorMetadata::toColumnHandle),
                table.getCompressionType());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        long tableId = ((RaptorInsertTableHandle) insertHandle).getTableId();

        transaction.insertChunks(tableId, deserializeChunks(fragments));

        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return CHUNK_ROW_ID_HANDLE;
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        transaction.registerTable(table.getTableId());
        return new RaptorTableHandle(table.getTableId(), OptionalLong.of(transaction.getTransactionId()));
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        long tableId = table.getTableId();

        ImmutableSet.Builder<Long> oldChunkIds = ImmutableSet.builder();
        ImmutableList.Builder<ChunkInfo> newChunks = ImmutableList.builder();

        fragments.stream()
                .map(fragment -> CHUNK_DELTA_CODEC.fromJson(fragment.getBytes()))
                .forEach(delta -> {
                    oldChunkIds.addAll(delta.getOldChunkIds());
                    newChunks.addAll(delta.getNewChunks());
                });

        transaction.deleteChunks(tableId, oldChunkIds.build());
        transaction.insertChunks(tableId, newChunks.build());
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        long schemaId = transaction.getSchemaId(viewName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(viewName.getSchemaName()));

        Optional<Long> viewId = transaction.getViewId(viewName);
        if (viewId.isPresent()) {
            if (!replace) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            transaction.dropView(viewId.get());
        }

        transaction.createView(
                schemaId,
                viewName.getTableName(),
                viewData,
                System.currentTimeMillis(),
                Optional.empty());
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        transaction.dropView(transaction.getViewId(viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName)));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName.map(transaction::listViews)
                .orElseGet(transaction::listViews);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> viewNames;
        if (prefix.getTableName() != null) {
            viewNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            viewNames = listViews(session, Optional.ofNullable(prefix.getSchemaName()));
        }

        // TODO: this should fetch all views at once
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (SchemaTableName viewName : viewNames) {
            transaction.getViewId(viewName)
                    .map(transaction::getViewInfo)
                    .ifPresent(view -> map.put(viewName, new ConnectorViewDefinition(
                            viewName, Optional.empty(), view.getViewData())));
        }
        return map.build();
    }

    private long getOrCreateDistribution(List<ColumnMetadata> columns, Map<String, Object> properties)
    {
        Map<String, ColumnMetadata> columnMap = uniqueIndex(columns, ColumnMetadata::getName);

        // bucket columns and types
        List<String> bucketColumns = getBucketColumns(properties);
        for (String column : bucketColumns) {
            if (!columnMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Bucketing column does not exist: " + column);
            }
        }

        List<Type> bucketColumnTypes = bucketColumns.stream()
                .map(columnMap::get)
                .map(ColumnMetadata::getType)
                .peek(HashingBucketFunction::validateBucketType)
                .collect(toImmutableList());

        // shared distribution
        String distributionName = getDistributionName(properties);
        if (distributionName != null) {
            DistributionInfo distribution = transaction.getDistributionInfo(distributionName)
                    .orElseThrow(() -> new PrestoException(INVALID_TABLE_PROPERTY, "Distribution does not exist: " + distributionName));

            OptionalInt bucketCount = getBucketCount(properties);
            if (!bucketCount.isPresent()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKET_COUNT_PROPERTY, DISTRIBUTION_NAME_PROPERTY));
            }
            if (bucketCount.getAsInt() != distribution.getBucketCount()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must match distribution bucket count: " + distribution.getBucketCount());
            }

            if (bucketColumns.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKETED_ON_PROPERTY, DISTRIBUTION_NAME_PROPERTY));
            }
            if (!bucketColumnTypes.equals(distribution.getColumnTypes())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket column types must match distribution: " +
                        distribution.getColumnTypes().stream()
                                .map(Type::getDisplayName)
                                .collect(joining(", ")));
            }

            return distribution.getDistributionId();
        }

        // anonymous distribution
        Set<Long> nodes = nodeSupplier.getRequiredWorkerNodes().stream()
                .map(RaptorNode::getNodeId)
                .collect(toImmutableSet());

        int bucketCount = nodes.size() * 8; // TODO: make this configurable

        // bucket count
        OptionalInt declaredBucketCount = getBucketCount(properties);
        if (declaredBucketCount.isPresent()) {
            bucketCount = declaredBucketCount.getAsInt();
            if (bucketCount == 0) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must be greater than zero");
            }
            if (bucketCount > MAX_BUCKETS) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must be no more than " + MAX_BUCKETS);
            }
        }
        else if (!bucketColumns.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKET_COUNT_PROPERTY, BUCKETED_ON_PROPERTY));
        }

        // create distribution
        return transaction.createDistribution(bucketColumnTypes, bucketNodes(nodes, bucketCount));
    }

    private static <T> OptionalInt listOrdinal(List<T> list, T value)
    {
        int index = list.indexOf(value);
        return (index == -1) ? OptionalInt.empty() : OptionalInt.of(index + 1);
    }

    private static List<ChunkInfo> deserializeChunks(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(Slice::getBytes)
                .map(CHUNK_INFO_CODEC::fromJson)
                .collect(toImmutableList());
    }

    private static RaptorColumnHandle toColumnHandle(ColumnInfo column)
    {
        return new RaptorColumnHandle(
                column.getColumnId(),
                column.getColumnName(),
                column.getType(),
                column.getComment());
    }

    private static ColumnMetadata toColumnMetadata(ColumnInfo column)
    {
        return new ColumnMetadata(
                column.getColumnName(),
                column.getType(),
                column.getComment().orElse(null),
                false);
    }

    private static ColumnMetadata toColumnMetadata(RaptorColumnHandle column)
    {
        return new ColumnMetadata(
                column.getColumnName(),
                column.getColumnType(),
                column.getComment().orElse(null),
                column.isHidden());
    }

    private static Optional<RaptorColumnHandle> getTemporalColumnHandle(String temporalColumn, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        if (temporalColumn == null) {
            return Optional.empty();
        }

        RaptorColumnHandle handle = columnHandleMap.get(temporalColumn);
        if (handle == null) {
            throw new PrestoException(NOT_FOUND, "Temporal column does not exist: " + temporalColumn);
        }
        return Optional.of(handle);
    }

    private static List<RaptorColumnHandle> getSortColumnHandles(List<String> sortColumns, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        for (String column : sortColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Ordering column does not exist: " + column);
            }
        }
        return sortColumns.stream()
                .map(columnHandleMap::get)
                .collect(toImmutableList());
    }
}
