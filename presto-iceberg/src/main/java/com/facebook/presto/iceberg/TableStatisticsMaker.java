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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.Partition.toMap;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;

public class TableStatisticsMaker
{
    private static final Logger log = Logger.get(TableStatisticsMaker.class);

    private static final String ICEBERG_APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";
    private final TypeManager typeManager;
    private final Table icebergTable;

    private TableStatisticsMaker(TypeManager typeManager, Table icebergTable)
    {
        this.typeManager = typeManager;
        this.icebergTable = icebergTable;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, Constraint constraint, IcebergTableHandle tableHandle, Table icebergTable, List<IcebergColumnHandle> columns)
    {
        return new TableStatisticsMaker(typeManager, icebergTable).makeTableStatistics(tableHandle, constraint, columns);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle, Constraint constraint, List<IcebergColumnHandle> selectedColumns)
    {
        if (!tableHandle.getSnapshotId().isPresent() || constraint.getSummary().isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        TupleDomain<IcebergColumnHandle> intersection = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast)
                .intersect(tableHandle.getPredicate());

        if (intersection.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        List<Types.NestedField> columns = icebergTable.schema().columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        List<Type> icebergPartitionTypes = partitionTypes(partitionFields, idToTypeMapping);
        List<IcebergColumnHandle> columnHandles = getColumns(icebergTable.schema(), typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

        ImmutableMap.Builder<Integer, ColumnFieldDetails> idToDetailsBuilder = ImmutableMap.builder();
        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            Type type = icebergPartitionTypes.get(index);
            idToDetailsBuilder.put(field.sourceId(), new ColumnFieldDetails(
                    field,
                    idToColumnHandle.get(field.sourceId()),
                    type,
                    toPrestoType(type, typeManager),
                    type.typeId().javaClass()));
        }
        Map<Integer, ColumnFieldDetails> idToDetails = idToDetailsBuilder.build();

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .select(selectedColumns.stream().map(IcebergColumnHandle::getName).collect(Collectors.toList()))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();

        Partition summary = null;
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                if (!dataFileMatches(
                        dataFile,
                        constraint,
                        idToTypeMapping,
                        partitionFields,
                        idToDetails)) {
                    continue;
                }

                if (summary == null) {
                    summary = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(idToTypeMapping, dataFile.lowerBounds()),
                            toMap(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                }
                else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(dataFile.recordCount());
                    summary.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, toMap(idToTypeMapping, dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(summary, partitionFields, toMap(idToTypeMapping, dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    summary.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(summary, dataFile.columnSizes());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (summary == null) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        // get NDVs from statistics file(s)
        ImmutableMap.Builder<Integer, Long> ndvByColumnId = ImmutableMap.builder();
        Set<Integer> remainingColumnIds = new HashSet<>(idToColumnHandle.keySet());

        getLatestStatisticsFile(icebergTable, tableHandle.getSnapshotId()).ifPresent(statisticsFile -> {
            Map<Integer, BlobMetadata> thetaBlobsByFieldId = statisticsFile.blobMetadata().stream()
                    .filter(blobMetadata -> blobMetadata.type().equals(APACHE_DATASKETCHES_THETA_V1))
                    .filter(blobMetadata -> {
                        try {
                            return remainingColumnIds.contains(getOnlyElement(blobMetadata.fields()));
                        }
                        catch (IllegalArgumentException e) {
                            throw new PrestoException(ICEBERG_INVALID_METADATA,
                                    format("blob metadata for blob type %s in statistics file %s must contain only one field. Found %d fields",
                                            APACHE_DATASKETCHES_THETA_V1, statisticsFile.path(), blobMetadata.fields().size()));
                        }
                    })
                    .collect(toImmutableMap(blobMetadata -> getOnlyElement(blobMetadata.fields()), identity()));

            for (Map.Entry<Integer, BlobMetadata> entry : thetaBlobsByFieldId.entrySet()) {
                int fieldId = entry.getKey();
                BlobMetadata blobMetadata = entry.getValue();
                String ndv = blobMetadata.properties().get(ICEBERG_APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                if (ndv == null) {
                    log.debug("Blob %s is missing %s property", blobMetadata.type(), ICEBERG_APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                    remainingColumnIds.remove(fieldId);
                }
                else {
                    remainingColumnIds.remove(fieldId);
                    ndvByColumnId.put(fieldId, parseLong(ndv));
                }
            }
        });
        Map<Integer, Long> ndvById = ndvByColumnId.build();

        double recordCount = summary.getRecordCount();
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(recordCount));
        result.setTotalSize(Estimate.of(summary.getSize()));
        for (IcebergColumnHandle columnHandle : selectedColumns) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                columnBuilder.setRange(Optional.of(new DoubleRange(((Number) min).doubleValue(), ((Number) max).doubleValue())));
            }
            Optional.ofNullable(ndvById.get(fieldId)).ifPresent(ndv -> columnBuilder.setDistinctValuesCount(Estimate.of(ndv)));
            result.setColumnStatistics(columnHandle, columnBuilder.build());
        }
        return result.build();
    }

    private boolean dataFileMatches(
            DataFile dataFile,
            Constraint constraint,
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<PartitionField> partitionFields,
            Map<Integer, ColumnFieldDetails> fieldDetails)
    {
        return true;
    }

    private static Optional<StatisticsFile> getLatestStatisticsFile(Table table, Optional<Long> snapshotId)
    {
        if (table.statisticsFiles().isEmpty()) {
            return Optional.empty();
        }

        Map<Long, StatisticsFile> statsFileBySnapshot = table.statisticsFiles().stream()
                .collect(toImmutableMap(
                        StatisticsFile::snapshotId,
                        identity(),
                        (file1, file2) -> {
                            throw new PrestoException(
                                    ICEBERG_INVALID_METADATA,
                                    format("Table '%s' has duplicate statistics files '%s' and '%s' for snapshot ID %s",
                                            table, file1.path(), file2.path(), file1.snapshotId()));
                        }));

        return stream(snapshotWalk(table, snapshotId.orElse(table.currentSnapshot().snapshotId())))
                .map(statsFileBySnapshot::get)
                .filter(Objects::nonNull)
                .findFirst();
    }

    public static Iterator<Long> snapshotWalk(Table table, Long startSnapshot)
    {
        return new Iterator<Long>()
        {
            private Long currentSnapshot = startSnapshot;

            @Override
            public boolean hasNext()
            {
                return currentSnapshot != null && table.snapshot(currentSnapshot) != null;
            }

            @Override
            public Long next()
            {
                Snapshot snapshot = table.snapshot(currentSnapshot);
                currentSnapshot = snapshot.parentId();
                return snapshot.snapshotId();
            }
        };
    }

    private NullableValue makeNullableValue(com.facebook.presto.common.type.Type type, Object value)
    {
        return value == null ? NullableValue.asNull(type) : NullableValue.of(type, value);
    }

    public List<Type> partitionTypes(List<PartitionField> partitionFields, Map<Integer, Type.PrimitiveType> idToTypeMapping)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static class ColumnFieldDetails
    {
        private final PartitionField field;
        private final IcebergColumnHandle columnHandle;
        private final Type icebergType;
        private final com.facebook.presto.common.type.Type prestoType;
        private final Class<?> javaClass;

        public ColumnFieldDetails(PartitionField field, IcebergColumnHandle columnHandle, Type icebergType, com.facebook.presto.common.type.Type prestoType, Class<?> javaClass)
        {
            this.field = requireNonNull(field, "field is null");
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.icebergType = requireNonNull(icebergType, "icebergType is null");
            this.prestoType = requireNonNull(prestoType, "prestoType is null");
            this.javaClass = requireNonNull(javaClass, "javaClass is null");
        }

        public PartitionField getField()
        {
            return field;
        }

        public IcebergColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        public Type getIcebergType()
        {
            return icebergType;
        }

        public com.facebook.presto.common.type.Type getPrestoType()
        {
            return prestoType;
        }

        public Class<?> getJavaClass()
        {
            return javaClass;
        }
    }

    public void updateColumnSizes(Partition summary, Map<Integer, Long> addedColumnSizes)
    {
        Map<Integer, Long> columnSizes = summary.getColumnSizes();
        if (!summary.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : summary.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMinValues(), lowerBounds, nullCounts, recordCount, i -> (i > 0));
        updatePartitionedStats(summary, partitionFields, summary.getMinValues(), lowerBounds, i -> (i > 0));
    }

    private void updateSummaryMax(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMaxValues(), upperBounds, nullCounts, recordCount, i -> (i < 0));
        updatePartitionedStats(summary, partitionFields, summary.getMaxValues(), upperBounds, i -> (i < 0));
    }

    private void updatePartitionedStats(
            Partition summary,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate)
    {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (summary.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(summary.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }
}
