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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class PartitionTable
        implements SystemTable
{
    private final TypeManager typeManager;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private final List<Types.NestedField> nonPartitionPrimitiveColumns;
    private final Optional<RowType> partitionColumnType;
    private final List<com.facebook.presto.common.type.Type> partitionColumnTypes;
    private final List<com.facebook.presto.common.type.Type> resultTypes;
    private final Optional<RowType> dataColumnType;
    private final List<RowType> columnMetricTypes;
    private final ConnectorTableMetadata connectorTableMetadata;

    public PartitionTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.idToTypeMapping = icebergTable.schema().columns().stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, (column) -> column.type().asPrimitiveType()));

        List<Types.NestedField> columns = icebergTable.schema().columns();
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        this.partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema());
        if (partitionColumnType.isPresent()) {
            columnMetadataBuilder.add(new ColumnMetadata("partition", partitionColumnType.get()));
            this.partitionColumnTypes = partitionColumnType.get().getFields().stream()
                    .map(RowType.Field::getType)
                    .collect(toImmutableList());
        }
        else {
            this.partitionColumnTypes = ImmutableList.of();
        }

        Stream.of("record_count", "file_count", "total_size")
                .forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        this.dataColumnType = getMetricsColumnType(this.nonPartitionPrimitiveColumns);
        if (dataColumnType.isPresent()) {
            columnMetadataBuilder.add(new ColumnMetadata("data", dataColumnType.get()));
            this.columnMetricTypes = dataColumnType.get().getFields().stream()
                    .map(RowType.Field::getType)
                    .map(RowType.class::cast)
                    .collect(toImmutableList());
        }
        else {
            this.columnMetricTypes = ImmutableList.of();
        }

        ImmutableList<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        this.connectorTableMetadata = new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return connectorTableMetadata;
    }

    private Optional<RowType> getPartitionColumnType(List<PartitionField> fields, Schema schema)
    {
        List<RowType.Field> partitionFields = fields.stream()
                .map(field -> RowType.field(
                        field.name(),
                        toPrestoType(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(toImmutableList());
        if (partitionFields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(partitionFields));
    }

    private Optional<RowType> getMetricsColumnType(List<Types.NestedField> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> RowType.field(
                        column.name(),
                        RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("min"), toPrestoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("max"), toPrestoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("null_count"), BIGINT)))))
                .collect(toImmutableList());
        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (!snapshotId.isPresent()) {
            return new InMemoryRecordSet(resultTypes, ImmutableList.of()).cursor();
        }
        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId.get())
                .includeColumnStats();
        // TODO make the cursor lazy
        return buildRecordCursor(getPartitions(tableScan), icebergTable.spec().fields());
    }

    private Map<StructLikeWrapper, Partition> getPartitions(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapper, Partition> partitions = new HashMap<>();

            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                Types.StructType structType = fileScanTask.spec().partitionType();
                StructLike partitionStruct = dataFile.partition();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(partitionStruct);

                if (!partitions.containsKey(partitionWrapper)) {
                    Partition partition = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            partitionStruct,
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(dataFile.lowerBounds()),
                            toMap(dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                    partitions.put(partitionWrapper, partition);
                    continue;
                }

                Partition partition = partitions.get(partitionWrapper);
                partition.incrementFileCount();
                partition.incrementRecordCount(dataFile.recordCount());
                partition.incrementSize(dataFile.fileSizeInBytes());
                partition.updateMin(toMap(dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateMax(toMap(dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateNullCount(dataFile.nullValueCounts());
            }

            return partitions;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RecordCursor buildRecordCursor(Map<StructLikeWrapper, Partition> partitions, List<PartitionField> partitionFields)
    {
        List<Type> partitionTypes = partitionTypes(partitionFields);
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                .map(type -> type.typeId().javaClass())
                .collect(toImmutableList());

        ImmutableList.Builder<List<Object>> records = ImmutableList.builder();

        for (Partition partition : partitions.values()) {
            List<Object> row = new ArrayList<>();

            // add data for partition columns
            partitionColumnType.ifPresent(partitionColumnType -> {
                BlockBuilder partitionRowBlockBuilder = partitionColumnType.createBlockBuilder(null, 1);
                BlockBuilder partitionBlockBuilder = partitionRowBlockBuilder.beginBlockEntry();
                for (int i = 0; i < partitionColumnTypes.size(); i++) {
                    com.facebook.presto.common.type.Type trinoType = partitionColumnType.getFields().get(i).getType();
                    Object value = convert(partition.getValues().get(i, partitionColumnClass.get(i)), partitionTypes.get(i));
                    writeNativeValue(trinoType, partitionBlockBuilder, value);
                }
                partitionRowBlockBuilder.closeEntry();
                row.add(partitionColumnType.getObject(partitionRowBlockBuilder, 0));
            });

            // add the top level metrics.
            row.add(partition.getRecordCount());
            row.add(partition.getFileCount());
            row.add(partition.getSize());

            // add column level metrics
            dataColumnType.ifPresent(dataColumnType -> {
                if (!partition.hasValidColumnMetrics()) {
                    row.add(null);
                    return;
                }
                BlockBuilder dataRowBlockBuilder = dataColumnType.createBlockBuilder(null, 1);
                BlockBuilder dataBlockBuilder = dataRowBlockBuilder.beginBlockEntry();

                for (int i = 0; i < columnMetricTypes.size(); i++) {
                    Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                    Type.PrimitiveType type = idToTypeMapping.get(fieldId);
                    Object min = convert(partition.getMinValues().get(fieldId), type);
                    Object max = convert(partition.getMaxValues().get(fieldId), type);
                    Long nullCount = partition.getNullCounts().get(fieldId);
                    columnMetricTypes.get(i).writeObject(dataBlockBuilder, getColumnMetricBlock(columnMetricTypes.get(i), min, max, nullCount));
                }
                dataRowBlockBuilder.closeEntry();
                row.add(dataColumnType.getObject(dataRowBlockBuilder, 0));
            });

            records.add(row);
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private List<Type> partitionTypes(List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static Block getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount)
    {
        BlockBuilder rowBlockBuilder = columnMetricType.createBlockBuilder(null, 1);
        BlockBuilder builder = rowBlockBuilder.beginBlockEntry();
        List<RowType.Field> fields = columnMetricType.getFields();
        writeNativeValue(fields.get(0).getType(), builder, min);
        writeNativeValue(fields.get(1).getType(), builder, max);
        writeNativeValue(fields.get(2).getType(), builder, nullCount);

        rowBlockBuilder.closeEntry();
        return columnMetricType.getObject(rowBlockBuilder, 0);
    }

    private Map<Integer, Object> toMap(Map<Integer, ByteBuffer> idToMetricMap)
    {
        return Partition.toMap(idToTypeMapping, idToMetricMap);
    }

    public static Object convert(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (type instanceof Types.StringType) {
            return value.toString();
        }
        if (type instanceof Types.BinaryType) {
            // TODO the client sees the bytearray's tostring output instead of seeing actual bytes, needs to be fixed.
            return ((ByteBuffer) value).array();
        }
        if (type instanceof Types.FloatType) {
            return Float.floatToIntBits((Float) value);
        }
        return value;
    }
}
