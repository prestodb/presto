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
package com.facebook.presto.raptor;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.DeltaInfoPair;
import com.facebook.presto.raptor.metadata.Distribution;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardDeleteDelta;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.ViewResult;
import com.facebook.presto.raptor.storage.StorageTypeConverter;
import com.facebook.presto.raptor.systemtables.ColumnRangesSystemTable;
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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.raptor.RaptorBucketFunction.validateBucketType;
import static com.facebook.presto.raptor.RaptorColumnHandle.BUCKET_NUMBER_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static com.facebook.presto.raptor.RaptorColumnHandle.bucketNumberColumnHandle;
import static com.facebook.presto.raptor.RaptorColumnHandle.isHiddenColumn;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardRowIdHandle;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorSessionProperties.getExternalBatchId;
import static com.facebook.presto.raptor.RaptorSessionProperties.getOneSplitPerBucketThreshold;
import static com.facebook.presto.raptor.RaptorTableProperties.BUCKETED_ON_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.DISTRIBUTION_NAME_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.ORDERING_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.ORGANIZED_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.TABLE_SUPPORTS_DELTA_DELETE;
import static com.facebook.presto.raptor.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.getBucketColumns;
import static com.facebook.presto.raptor.RaptorTableProperties.getBucketCount;
import static com.facebook.presto.raptor.RaptorTableProperties.getDistributionName;
import static com.facebook.presto.raptor.RaptorTableProperties.getSortColumns;
import static com.facebook.presto.raptor.RaptorTableProperties.getTemporalColumn;
import static com.facebook.presto.raptor.RaptorTableProperties.isOrganized;
import static com.facebook.presto.raptor.RaptorTableProperties.isTableSupportsDeltaDelete;
import static com.facebook.presto.raptor.systemtables.ColumnRangesSystemTable.getSourceTable;
import static com.facebook.presto.raptor.util.DatabaseUtil.daoTransaction;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RaptorMetadata.class);

    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);
    private static final JsonCodec<ShardDeleteDelta> SHARD_DELETE_DELTA_CODEC = jsonCodec(ShardDeleteDelta.class);

    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final TypeManager typeManager;
    private final String connectorId;
    private final LongConsumer beginDeleteForTableId;

    private final AtomicReference<Long> currentTransactionId = new AtomicReference<>();

    public RaptorMetadata(String connectorId, IDBI dbi, ShardManager shardManager, TypeManager typeManager)
    {
        this(connectorId, dbi, shardManager, typeManager, tableId -> {});
    }

    public RaptorMetadata(
            String connectorId,
            IDBI dbi,
            ShardManager shardManager,
            TypeManager typeManager,
            LongConsumer beginDeleteForTableId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.beginDeleteForTableId = requireNonNull(beginDeleteForTableId, "beginDeleteForTableId is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private RaptorTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<TableColumn> tableColumns = dao.listTableColumns(table.getTableId());
        checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        return new RaptorTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId(),
                table.getDistributionId(),
                table.getDistributionName(),
                table.getBucketCount(),
                table.isOrganized(),
                OptionalLong.empty(),
                Optional.empty(),
                false,
                table.isTableSupportsDeltaDelete());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getSourceTable(tableName)
                .map(this::getTableHandle)
                .map(handle -> new ColumnRangesSystemTable(handle, dbi));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = (RaptorTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<TableColumn> tableColumns = dao.listTableColumns(handle.getTableId());
        if (tableColumns.isEmpty()) {
            throw new TableNotFoundException(tableName);
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        SortedMap<Integer, String> bucketing = new TreeMap<>();
        SortedMap<Integer, String> ordering = new TreeMap<>();

        for (TableColumn column : tableColumns) {
            if (column.isTemporal()) {
                properties.put(TEMPORAL_COLUMN_PROPERTY, column.getColumnName());
            }
            column.getBucketOrdinal().ifPresent(bucketOrdinal -> bucketing.put(bucketOrdinal, column.getColumnName()));
            column.getSortOrdinal().ifPresent(sortOrdinal -> ordering.put(sortOrdinal, column.getColumnName()));
        }

        if (!bucketing.isEmpty()) {
            properties.put(BUCKETED_ON_PROPERTY, ImmutableList.copyOf(bucketing.values()));
        }
        if (!ordering.isEmpty()) {
            properties.put(ORDERING_PROPERTY, ImmutableList.copyOf(ordering.values()));
        }

        handle.getBucketCount().ifPresent(bucketCount -> properties.put(BUCKET_COUNT_PROPERTY, bucketCount));
        handle.getDistributionName().ifPresent(distributionName -> properties.put(DISTRIBUTION_NAME_PROPERTY, distributionName));
        // Only display organization and table_supports_delta_delete property if set
        if (handle.isOrganized()) {
            properties.put(ORGANIZED_PROPERTY, true);
        }
        if (handle.isTableSupportsDeltaDelete()) {
            properties.put(TABLE_SUPPORTS_DELTA_DELETE, true);
        }

        List<ColumnMetadata> columns = tableColumns.stream()
                .map(TableColumn::toColumnMetadata)
                .collect(toCollection(ArrayList::new));

        columns.add(hiddenColumn(SHARD_UUID_COLUMN_NAME, SHARD_UUID_COLUMN_TYPE));

        if (handle.isBucketed()) {
            columns.add(hiddenColumn(BUCKET_NUMBER_COLUMN_NAME, INTEGER));
        }

        properties.putAll(getExtraProperties(handle.getTableId()));

        return new ConnectorTableMetadata(tableName, columns, properties.build());
    }

    protected Map<String, Object> getExtraProperties(long tableid)
    {
        return ImmutableMap.of();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            builder.put(tableColumn.getColumnName(), getRaptorColumnHandle(tableColumn));
        }

        RaptorColumnHandle uuidColumn = shardUuidColumnHandle(connectorId);
        builder.put(uuidColumn.getColumnName(), uuidColumn);

        if (raptorTableHandle.isBucketed()) {
            RaptorColumnHandle bucketNumberColumn = bucketNumberColumnHandle(connectorId);
            builder.put(bucketNumberColumn.getColumnName(), bucketNumberColumn);
        }

        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        RaptorColumnHandle column = (RaptorColumnHandle) columnHandle;

        if (isHiddenColumn(column.getColumnId())) {
            return hiddenColumn(column.getColumnName(), column.getColumnType());
        }

        return new ColumnMetadata(column.getColumnName(), column.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(prefix.getSchemaName(), prefix.getTableName())) {
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType());
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RaptorTableHandle handle = (RaptorTableHandle) table;
        ConnectorTableLayout layout = getTableLayout(session, handle, constraint.getSummary());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        RaptorTableLayoutHandle raptorHandle = (RaptorTableLayoutHandle) handle;
        return getTableLayout(session, raptorHandle.getTable(), raptorHandle.getConstraint());
    }

    private ConnectorTableLayout getTableLayout(ConnectorSession session, RaptorTableHandle handle, TupleDomain<ColumnHandle> constraint)
    {
        if (!handle.getDistributionId().isPresent()) {
            return new ConnectorTableLayout(new RaptorTableLayoutHandle(handle, constraint, Optional.empty()));
        }

        List<RaptorColumnHandle> bucketColumnHandles = getBucketColumnHandles(handle.getTableId());

        RaptorPartitioningHandle partitioning = getPartitioningHandle(handle.getDistributionId().getAsLong());

        boolean oneSplitPerBucket = handle.getBucketCount().getAsInt() >= getOneSplitPerBucketThreshold(session);

        return new ConnectorTableLayout(
                new RaptorTableLayoutHandle(handle, constraint, Optional.of(partitioning)),
                Optional.empty(),
                TupleDomain.all(),
                Optional.of(new ConnectorTablePartitioning(
                        partitioning,
                        ImmutableList.copyOf(bucketColumnHandles))),
                oneSplitPerBucket ? Optional.of(ImmutableSet.copyOf(bucketColumnHandles)) : Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata metadata)
    {
        ImmutableMap.Builder<String, RaptorColumnHandle> map = ImmutableMap.builder();
        long columnId = 1;
        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);
        for (ColumnMetadata column : metadata.getColumns()) {
            checkState(storageTypeConverter.toStorageType(column.getType()) != null, "storage type cannot be null");
            map.put(column.getName(), new RaptorColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnId++;
        }

        Optional<DistributionInfo> distribution = getOrCreateDistribution(map.build(), metadata.getProperties());
        if (!distribution.isPresent()) {
            return Optional.empty();
        }

        List<String> partitionColumns = distribution.get().getBucketColumns().stream()
                .map(RaptorColumnHandle::getColumnName)
                .collect(toList());

        ConnectorPartitioningHandle partitioning = getPartitioningHandle(distribution.get().getDistributionId());
        return Optional.of(new ConnectorNewTableLayout(partitioning, partitionColumns));
    }

    private RaptorPartitioningHandle getPartitioningHandle(long distributionId)
    {
        return new RaptorPartitioningHandle(distributionId, shardManager.getBucketAssignments(distributionId));
    }

    private Optional<DistributionInfo> getOrCreateDistribution(Map<String, RaptorColumnHandle> columnHandleMap, Map<String, Object> properties)
    {
        OptionalInt bucketCount = getBucketCount(properties);
        List<RaptorColumnHandle> bucketColumnHandles = getBucketColumnHandles(getBucketColumns(properties), columnHandleMap);

        if (bucketCount.isPresent() && bucketColumnHandles.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKETED_ON_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        if (!bucketCount.isPresent() && !bucketColumnHandles.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKET_COUNT_PROPERTY, BUCKETED_ON_PROPERTY));
        }
        ImmutableList.Builder<Type> bucketColumnTypes = ImmutableList.builder();
        for (RaptorColumnHandle column : bucketColumnHandles) {
            validateBucketType(column.getColumnType());
            bucketColumnTypes.add(column.getColumnType());
        }

        long distributionId;
        String distributionName = getDistributionName(properties);
        if (distributionName != null) {
            if (bucketColumnHandles.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKETED_ON_PROPERTY, DISTRIBUTION_NAME_PROPERTY));
            }

            Distribution distribution = dao.getDistribution(distributionName);
            if (distribution == null) {
                if (!bucketCount.isPresent()) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Distribution does not exist and bucket count is not specified");
                }
                distribution = getOrCreateDistribution(distributionName, bucketColumnTypes.build(), bucketCount.getAsInt());
            }
            distributionId = distribution.getId();

            if (bucketCount.isPresent() && (distribution.getBucketCount() != bucketCount.getAsInt())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must match distribution");
            }
            if (!distribution.getColumnTypes().equals(bucketColumnTypes.build())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket column types must match distribution");
            }
        }
        else if (bucketCount.isPresent()) {
            String types = Distribution.serializeColumnTypes(bucketColumnTypes.build());
            distributionId = dao.insertDistribution(null, types, bucketCount.getAsInt());
        }
        else {
            return Optional.empty();
        }

        shardManager.createBuckets(distributionId, bucketCount.getAsInt());

        return Optional.of(new DistributionInfo(distributionId, bucketCount.getAsInt(), bucketColumnHandles));
    }

    private Distribution getOrCreateDistribution(String name, List<Type> columnTypes, int bucketCount)
    {
        String types = Distribution.serializeColumnTypes(columnTypes);
        runIgnoringConstraintViolation(() -> dao.insertDistribution(name, types, bucketCount));

        Distribution distribution = dao.getDistribution(name);
        if (distribution == null) {
            throw new PrestoException(RAPTOR_ERROR, "Distribution does not exist after insert");
        }
        return distribution;
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
        RaptorTableHandle raptorHandle = (RaptorTableHandle) tableHandle;
        shardManager.dropTable(raptorHandle.getTableId());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        runTransaction(dbi, (handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            return null;
        });
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;

        // Always add new columns to the end.
        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        TableColumn lastColumn = existingColumns.get(existingColumns.size() - 1);
        long columnId = lastColumn.getColumnId() + 1;
        int ordinalPosition = lastColumn.getOrdinalPosition() + 1;

        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);
        checkState(storageTypeConverter.toStorageType(column.getType()) != null, "storage type cannot be null");
        String type = column.getType().getTypeSignature().toString();
        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.insertColumn(table.getTableId(), columnId, column.getName(), ordinalPosition, type, null, null);
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });

        shardManager.addColumn(table.getTableId(), new ColumnInfo(columnId, column.getType()));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        RaptorColumnHandle sourceColumn = (RaptorColumnHandle) source;
        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.renameColumn(table.getTableId(), sourceColumn.getColumnId(), target);
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        RaptorColumnHandle raptorColumn = (RaptorColumnHandle) column;

        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        if (existingColumns.size() <= 1) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the only column in a table");
        }
        long maxColumnId = existingColumns.stream().mapToLong(TableColumn::getColumnId).max().getAsLong();
        if (raptorColumn.getColumnId() == maxColumnId) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the column which has the largest column ID in the table");
        }

        if (getBucketColumnHandles(table.getTableId()).contains(column)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop bucket columns");
        }

        Optional.ofNullable(dao.getTemporalColumnId(table.getTableId())).ifPresent(tempColumnId -> {
            if (raptorColumn.getColumnId() == tempColumnId) {
                throw new PrestoException(NOT_SUPPORTED, "Cannot drop the temporal column");
            }
        });

        if (getSortColumnHandles(table.getTableId()).contains(raptorColumn)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop sort columns");
        }

        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.dropColumn(table.getTableId(), raptorColumn.getColumnId());
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });

        // TODO: drop column from index table
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        if (viewExists(session, tableMetadata.getTable())) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + tableMetadata.getTable());
        }

        runExtraEligibilityCheck(session, tableMetadata);

        Optional<RaptorPartitioningHandle> partitioning = layout
                .map(ConnectorNewTableLayout::getPartitioning)
                .map(RaptorPartitioningHandle.class::cast);

        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        long columnId = 1;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnTypes.add(column.getType());
            columnId++;
        }
        Map<String, RaptorColumnHandle> columnHandleMap = Maps.uniqueIndex(columnHandles.build(), RaptorColumnHandle::getColumnName);

        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(tableMetadata.getProperties()), columnHandleMap);
        Optional<RaptorColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(tableMetadata.getProperties()), columnHandleMap);

        if (temporalColumnHandle.isPresent()) {
            RaptorColumnHandle column = temporalColumnHandle.get();
            if (!column.getColumnType().equals(TIMESTAMP) && !column.getColumnType().equals(DATE)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type timestamp or date: " + column.getColumnName());
            }
        }

        boolean organized = isOrganized(tableMetadata.getProperties());
        if (organized) {
            if (temporalColumnHandle.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Table with temporal columns cannot be organized");
            }
            if (sortColumnHandles.isEmpty()) {
                throw new PrestoException(NOT_SUPPORTED, "Table organization requires an ordering");
            }
        }

        long transactionId = shardManager.beginTransaction();

        setTransactionId(transactionId);

        Optional<DistributionInfo> distribution = partitioning.map(handle ->
                getDistributionInfo(handle.getDistributionId(), columnHandleMap, tableMetadata.getProperties()));

        return new RaptorOutputTableHandle(
                connectorId,
                transactionId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                temporalColumnHandle,
                distribution.map(info -> OptionalLong.of(info.getDistributionId())).orElse(OptionalLong.empty()),
                distribution.map(info -> OptionalInt.of(info.getBucketCount())).orElse(OptionalInt.empty()),
                distribution.map(DistributionInfo::getBucketColumns).orElse(ImmutableList.of()),
                organized,
                isTableSupportsDeltaDelete(tableMetadata.getProperties()),
                tableMetadata.getProperties().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
    }

    private DistributionInfo getDistributionInfo(long distributionId, Map<String, RaptorColumnHandle> columnHandleMap, Map<String, Object> properties)
    {
        Distribution distribution = dao.getDistribution(distributionId);
        if (distribution == null) {
            throw new PrestoException(RAPTOR_ERROR, "Distribution ID does not exist: " + distributionId);
        }
        List<RaptorColumnHandle> bucketColumnHandles = getBucketColumnHandles(getBucketColumns(properties), columnHandleMap);
        return new DistributionInfo(distributionId, distribution.getBucketCount(), bucketColumnHandles);
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
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : sortColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Ordering column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    private static List<RaptorColumnHandle> getBucketColumnHandles(List<String> bucketColumns, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : bucketColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Bucketing column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    protected void runExtraEligibilityCheck(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // no-op
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        RaptorOutputTableHandle table = (RaptorOutputTableHandle) outputTableHandle;
        long transactionId = table.getTransactionId();
        long updateTime = session.getStartTime();

        long newTableId = runTransaction(dbi, (dbiHandle, status) -> {
            MetadataDao dao = dbiHandle.attach(MetadataDao.class);

            Long distributionId = table.getDistributionId().isPresent() ? table.getDistributionId().getAsLong() : null;
            // TODO: update default value of organization_enabled to true
            long tableId = dao.insertTable(table.getSchemaName(), table.getTableName(), true, table.isOrganized(), distributionId, updateTime, table.isTableSupportsDeltaDelete());

            runExtraCreateTableStatement(tableId, dbiHandle, table.getProperties());

            List<RaptorColumnHandle> sortColumnHandles = table.getSortColumnHandles();
            List<RaptorColumnHandle> bucketColumnHandles = table.getBucketColumnHandles();

            for (int i = 0; i < table.getColumnTypes().size(); i++) {
                RaptorColumnHandle column = table.getColumnHandles().get(i);

                int columnId = i + 1;
                String type = table.getColumnTypes().get(i).getTypeSignature().toString();
                Integer sortPosition = sortColumnHandles.contains(column) ? sortColumnHandles.indexOf(column) : null;
                Integer bucketPosition = bucketColumnHandles.contains(column) ? bucketColumnHandles.indexOf(column) : null;

                dao.insertColumn(tableId, columnId, column.getColumnName(), i, type, sortPosition, bucketPosition);

                if (table.getTemporalColumnHandle().isPresent() && table.getTemporalColumnHandle().get().equals(column)) {
                    dao.updateTemporalColumnId(tableId, columnId);
                }
            }

            return tableId;
        });

        List<ColumnInfo> columns = table.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        OptionalLong temporalColumnId = table.getTemporalColumnHandle().map(RaptorColumnHandle::getColumnId)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());

        // TODO: refactor this to avoid creating an empty table on failure
        shardManager.createTable(newTableId, columns, table.getBucketCount().isPresent(), temporalColumnId, table.isTableSupportsDeltaDelete());
        shardManager.commitShards(transactionId, newTableId, columns, parseFragments(fragments), Optional.empty(), updateTime);

        clearRollback();

        return Optional.empty();
    }

    protected void runExtraCreateTableStatement(long tableId, Handle dbiHandle, Map<String, String> extraProperties)
    {
        // no-op
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = (RaptorTableHandle) tableHandle;
        long tableId = handle.getTableId();

        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (TableColumn column : dao.listTableColumns(tableId)) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getColumnName(), column.getColumnId(), column.getDataType()));
            columnTypes.add(column.getDataType());
        }

        long transactionId = shardManager.beginTransaction();

        setTransactionId(transactionId);

        Optional<String> externalBatchId = getExternalBatchId(session);
        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        List<RaptorColumnHandle> bucketColumnHandles = getBucketColumnHandles(tableId);

        Optional<RaptorColumnHandle> temporalColumnHandle = Optional.ofNullable(dao.getTemporalColumnId(tableId))
                .map(temporalColumnId -> getOnlyElement(columnHandles.build().stream()
                        .filter(columnHandle -> columnHandle.getColumnId() == temporalColumnId)
                        .collect(toList())));

        return new RaptorInsertTableHandle(connectorId,
                transactionId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                handle.getBucketCount(),
                bucketColumnHandles,
                temporalColumnHandle);
    }

    private List<RaptorColumnHandle> getSortColumnHandles(long tableId)
    {
        return dao.listSortColumns(tableId).stream()
                .map(this::getRaptorColumnHandle)
                .collect(toList());
    }

    private List<RaptorColumnHandle> getBucketColumnHandles(long tableId)
    {
        return dao.listBucketColumns(tableId).stream()
                .map(this::getRaptorColumnHandle)
                .collect(toList());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        RaptorInsertTableHandle handle = (RaptorInsertTableHandle) insertHandle;
        long transactionId = handle.getTransactionId();
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = handle.getExternalBatchId();
        List<ColumnInfo> columns = handle.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());
        long updateTime = session.getStartTime();

        Collection<ShardInfo> shards = parseFragments(fragments);
        log.info("Committing insert into tableId %s (queryId: %s, shards: %s, columns: %s)", handle.getTableId(), session.getQueryId(), shards.size(), columns.size());
        shardManager.commitShards(transactionId, tableId, columns, shards, externalBatchId, updateTime);

        clearRollback();

        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return shardRowIdHandle(connectorId);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = (RaptorTableHandle) tableHandle;

        beginDeleteForTableId.accept(handle.getTableId());

        long transactionId = shardManager.beginTransaction();

        setTransactionId(transactionId);

        Map<String, Type> columnTypes = dao.listTableColumns(handle.getTableId()).stream().collect(Collectors.toMap(k -> String.valueOf(k.getColumnId()), TableColumn::getDataType));

        return new RaptorTableHandle(
                connectorId,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableId(),
                handle.getDistributionId(),
                handle.getDistributionName(),
                handle.getBucketCount(),
                handle.isOrganized(),
                OptionalLong.of(transactionId),
                Optional.of(columnTypes),
                true,
                handle.isTableSupportsDeltaDelete());
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        RaptorTableHandle table = (RaptorTableHandle) tableHandle;
        long transactionId = table.getTransactionId().getAsLong();
        long tableId = table.getTableId();

        List<ColumnInfo> columns = getColumnHandles(session, tableHandle).values().stream()
                .map(RaptorColumnHandle.class::cast)
                .map(ColumnInfo::fromHandle).collect(toList());

        if (table.isTableSupportsDeltaDelete()) {
            ImmutableMap.Builder<UUID, DeltaInfoPair> shardMapBuilder = ImmutableMap.builder();

            fragments.stream()
                    .map(fragment -> SHARD_DELETE_DELTA_CODEC.fromJson(fragment.getBytes()))
                    .forEach(delta -> shardMapBuilder.put(delta.getOldShardUuid(), delta.getDeltaInfoPair()));
            OptionalLong updateTime = OptionalLong.of(session.getStartTime());

            log.info("Finishing delete for tableId %s (affected shardUuid: %s)", tableId, shardMapBuilder.build().size());
            shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMapBuilder.build(), updateTime);
        }
        else {
            ImmutableSet.Builder<UUID> oldShardUuidsBuilder = ImmutableSet.builder();
            ImmutableList.Builder<ShardInfo> newShardsBuilder = ImmutableList.builder();

            fragments.stream()
                    .map(fragment -> SHARD_DELTA_CODEC.fromJson(fragment.getBytes()))
                    .forEach(delta -> {
                        oldShardUuidsBuilder.addAll(delta.getOldShardUuids());
                        newShardsBuilder.addAll(delta.getNewShards());
                    });

            Set<UUID> oldShardUuids = oldShardUuidsBuilder.build();
            List<ShardInfo> newShards = newShardsBuilder.build();
            OptionalLong updateTime = OptionalLong.of(session.getStartTime());

            log.info("Finishing delete for tableId %s (removed: %s, rewritten: %s)", tableId, oldShardUuids.size() - newShards.size(), newShards.size());
            shardManager.replaceShardUuids(transactionId, tableId, columns, oldShardUuids, newShards, updateTime);
        }
        clearRollback();
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        return false;
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        SchemaTableName viewName = viewMetadata.getTable();
        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();

        if (getTableHandle(viewName) != null) {
            throw new PrestoException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            daoTransaction(dbi, MetadataDao.class, dao -> {
                dao.dropView(schemaName, tableName);
                dao.insertView(schemaName, tableName, viewData);
            });
            return;
        }

        try {
            dao.insertView(schemaName, tableName, viewData);
        }
        catch (PrestoException e) {
            if (viewExists(session, viewName)) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            throw e;
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (!viewExists(session, viewName)) {
            throw new ViewNotFoundException(viewName);
        }
        dao.dropView(viewName.getSchemaName(), viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listViews(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (ViewResult view : dao.getViews(prefix.getSchemaName(), prefix.getTableName())) {
            map.put(view.getName(), new ConnectorViewDefinition(view.getName(), Optional.empty(), view.getData()));
        }
        return map.build();
    }

    private boolean viewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return !getViews(session, viewName.toSchemaTablePrefix()).isEmpty();
    }

    private RaptorColumnHandle getRaptorColumnHandle(TableColumn tableColumn)
    {
        return new RaptorColumnHandle(connectorId, tableColumn.getColumnName(), tableColumn.getColumnId(), tableColumn.getDataType());
    }

    private static Collection<ShardInfo> parseFragments(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> SHARD_INFO_CODEC.fromJson(fragment.getBytes()))
                .collect(toList());
    }

    private static ColumnMetadata hiddenColumn(String name, Type type)
    {
        return new ColumnMetadata(name, type, null, true);
    }

    private void setTransactionId(long transactionId)
    {
        checkState(currentTransactionId.compareAndSet(null, transactionId), "current transaction ID already set");
    }

    private void clearRollback()
    {
        currentTransactionId.set(null);
    }

    public void rollback()
    {
        Long transactionId = currentTransactionId.getAndSet(null);
        if (transactionId != null) {
            shardManager.rollbackTransaction(transactionId);
        }
    }

    private static class DistributionInfo
    {
        private final long distributionId;
        private final int bucketCount;
        private final List<RaptorColumnHandle> bucketColumns;

        public DistributionInfo(long distributionId, int bucketCount, List<RaptorColumnHandle> bucketColumns)
        {
            this.distributionId = distributionId;
            this.bucketCount = bucketCount;
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
        }

        public long getDistributionId()
        {
            return distributionId;
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        public List<RaptorColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }
    }
}
