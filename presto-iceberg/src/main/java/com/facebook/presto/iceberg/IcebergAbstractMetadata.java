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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveWrittenPartitions;
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
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public abstract class IcebergAbstractMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergAbstractMetadata.class);

    protected final TypeManager typeManager;
    protected final JsonCodec<CommitTaskData> commitTaskCodec;

    protected final Map<String, Optional<Long>> snapshotIds = new ConcurrentHashMap<>();

    protected Transaction transaction;

    public IcebergAbstractMetadata(TypeManager typeManager, JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new IcebergTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    protected Optional<SystemTable> getIcebergSystemTable(SchemaTableName tableName, org.apache.iceberg.Table table)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        Optional<Long> snapshotId = resolveSnapshotIdByName(table, name);

        switch (name.getTableType()) {
            case DATA:
                break;
            case HISTORY:
                if (name.getSnapshotId().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, "Snapshot ID not supported for history table: " + systemTableName);
                }
                // make sure Hadoop FileSystem initialized in classloader-safe context
                table.refresh();
                return Optional.of(new HistoryTable(systemTableName, table));
            case SNAPSHOTS:
                if (name.getSnapshotId().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, "Snapshot ID not supported for snapshots table: " + systemTableName);
                }
                table.refresh();
                return Optional.of(new SnapshotsTable(systemTableName, typeManager, table));
            case PARTITIONS:
                return Optional.of(new PartitionTable(systemTableName, typeManager, table, snapshotId));
            case MANIFESTS:
                return Optional.of(new ManifestsTable(systemTableName, table, snapshotId));
            case FILES:
                return Optional.of(new FilesTable(systemTableName, table, snapshotId, typeManager));
            case PROPERTIES:
                return Optional.of(new PropertiesTable(systemTableName, table));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    protected abstract ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table);

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null ? singletonList(prefix.toSchemaTableName()) : listTables(session, Optional.of(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                log.warn(String.format("table disappeared during listing operation: %s", e.getMessage()));
            }
            catch (UnknownTableTypeException e) {
                log.warn(String.format("%s: Unknown table type of table %s", e.getMessage(), table.getTableName()));
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .setHidden(false)
                .build();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, ImmutableSet.of(), computedStatistics);
    }

    protected ConnectorInsertTableHandle beginIcebergTableInsert(IcebergTableHandle table, org.apache.iceberg.Table icebergTable)
    {
        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<Slice> deprecatedFragments, Collection<ComputedStatistics> computedStatistics)
    {
        checkArgument(deprecatedFragments.isEmpty(), "Deprecated fragments are unsupported");
        if (fragments.isEmpty()) {
            transaction.commitTransaction();
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        org.apache.iceberg.Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new IcebergColumnHandle(0, "$row_id", BIGINT, Optional.empty());
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    protected List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
    {
        return table.schema().columns().stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(toPrestoType(column.type(), typeManager))
                        .setComment(Optional.ofNullable(column.doc()))
                        .setHidden(false)
                        .build())
                .collect(toImmutableList());
    }

    protected ImmutableMap<String, Object> createMetadataProperties(org.apache.iceberg.Table icebergTable)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return properties.build();
    }

    protected static Schema toIcebergSchema(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                Type type = toIcebergType(column.getType());
                Types.NestedField field = column.isNullable()
                        ? Types.NestedField.optional(index, column.getName(), type, column.getComment())
                        : Types.NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }
}
