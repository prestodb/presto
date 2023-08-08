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
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.samples.SampleUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.useSampleStatistics;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.Partition.toMap;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

public class TableStatisticsMaker
{
    private static final Logger LOG = Logger.get(TableStatisticsMaker.class);
    private final TypeManager typeManager;
    private final Table icebergTable;

    private final ConnectorSession session;
    private final HdfsEnvironment hdfsEnvironment;

    private TableStatisticsMaker(TypeManager typeManager, Table icebergTable, ConnectorSession session, HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = typeManager;
        this.icebergTable = icebergTable;
        this.session = session;
        this.hdfsEnvironment = hdfsEnvironment;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, Constraint constraint, IcebergTableHandle tableHandle, Table icebergTable, ConnectorSession session, HdfsEnvironment hdfsEnvironment)
    {
        return new TableStatisticsMaker(typeManager, icebergTable, session, hdfsEnvironment).makeTableStatistics(tableHandle, constraint);
    }

    private static class TableDiff
            implements Comparable<TableDiff>
    {
        public long recordsAdded;
        public long recordsDeleted;

        @Override
        public int compareTo(@NotNull TableStatisticsMaker.TableDiff o)
        {
            // this calculation may eventually be changes to weight added/deleted records unevenly
            // due to the way the sample is created
            return Long.compare(this.recordsAdded + this.recordsDeleted, o.recordsAdded + o.recordsDeleted);
        }
    }

    private static TableDiff calculateDiff(Table table, long beginSnapshotId, long endSnapshotId)
    {
        TableDiff diff = new TableDiff();
        try (CloseableIterable<ChangelogScanTask> tasks = table.newIncrementalChangelogScan().fromSnapshotExclusive(beginSnapshotId).toSnapshot(endSnapshotId).planFiles()) {
            for (ChangelogScanTask task : tasks) {
                switch (task.operation()) {
                    case DELETE:
                        diff.recordsDeleted += task.estimatedRowsCount();
                        break;
                    case INSERT:
                        diff.recordsAdded += task.estimatedRowsCount();
                        break;
                }
            }
            return diff;
        }
        catch (IOException | IllegalArgumentException | IllegalStateException e) {
            LOG.error("failed to calculate diff", e);
            return diff;
        }
    }

    /**
     * There are two tables: "actual" and "sample" which have snapshots that occur at varying
     * points. e.g.
     * <br>
     * <code>
     * actual |------s1---------s2---------s3----------s4----s5--s6-s7------->
     * <br>
     * sample |---------s1-----------------------s2----------------------s3-->
     * </code>
     * <br>
     * The goal here is to calculate the snapshot s[1-3] which has the smallest delta to the
     * snapshot currently queried by the actual table.
     * <br>
     * For example, if the user queried "actual" at snapshot s5, which sample of the snapshot table
     * is going to most accurately represent the table @ s5?
     * If s4 added 10M rows, but s6 and s7 only added 10k rows, Sample s3 is likely going to
     * be more representative.
     * <br>
     * The algorithm is as follows:
     * <ol>
     *     <li>Calculate time of the snapshot, defined as S,  being queried, defined as t1</li>
     *     <li>
     *         Lookup the snapshots on the timeline of sample table which immediately precede
     *         and succeed t1 -- if either preceding or succeeding don't exist, return whichever one
     *         does exist, or none if no snapshot could be found.
     *         The preceding snapshot of the sample table, S2, occurs at t2.
     *         The succeeding snapshot of the sample table, S3, occurs at t3.
     *     </li>
     *     <li>
     *         Find the version of the actual table which occurs closest to time t2, Sa1. Calculate
     *         the changelog diff between Sa1-->S, diff_before
     *     </li>
     *     <li>
     *         4. Find the version of the actual table which occurs closest to time t3, Sa2.
     *         Calculate the changelog diff between S-->Sa2, diff_after
     *     </li>
     *     <li>
     *         Compare the size of the diff between diff_before and diff_after. If diff_before is
     *         smaller or equal to diff_after, choose sample @ S2,If diff_after is smaller, choose
     *         sample @ S3,
     *     </li>
     *
     * </ol>
     *
     * @param actualTable reference to the real iceberg table
     * @param actualTableHandle the presto {@link IcebergTableHandle} reference to the "actual"
     * iceberg table
     * @param sampleTable the reference to the Sample {@link Table}.
     * @return the snapshot id to use for the sample table.
     */
    public static Optional<Long> calculateSnapshotToUseFromSample(Table actualTable, IcebergTableHandle actualTableHandle, Table sampleTable)
    {
        return actualTableHandle.getSnapshotId().flatMap((ssid) -> {
            Snapshot actualSnap = actualTable.snapshot(ssid);
            Snapshot samplePrevSnap;
            try {
                samplePrevSnap = sampleTable.snapshot(SnapshotUtil.snapshotIdAsOfTime(sampleTable, actualSnap.timestampMillis()));
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                // no snapshot exists before this time, use the first version of the sample table after the timestamp
                try {
                    Snapshot nextSnapshot = SnapshotUtil.oldestAncestorAfter(sampleTable, actualSnap.timestampMillis());
                    if (nextSnapshot != null) {
                        return Optional.of(nextSnapshot.snapshotId());
                    }
                    return Optional.empty();
                }
                catch (IllegalArgumentException | IllegalStateException e2) {
                    LOG.warn("no sample table snapshots found before or after " + actualSnap.timestampMillis() + ". Did you create the samples table?", e);
                    return Optional.empty();
                }
            }
            Snapshot sampleNextSnap;
            try {
                sampleNextSnap = SnapshotUtil.snapshotAfter(sampleTable, samplePrevSnap.snapshotId());
            }
            catch (IllegalStateException | IllegalArgumentException | NullPointerException e) {
                // no sample snapshot which after this.
                return Optional.of(samplePrevSnap.snapshotId());
            }
            Snapshot actualPrevSnap;
            try {
                actualPrevSnap = actualTable.snapshot(SnapshotUtil.snapshotIdAsOfTime(actualTable, samplePrevSnap.timestampMillis()));
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                // there's no snapshot that exists which was created before this time. Does the table even exist?
                // ideally this shouldn't happen, as it means the sample table was created before the actual table
                // in the case it happens, just return nothing
                LOG.warn("no actual table snapshots found before " + actualSnap.timestampMillis() + ". How did you get here. Please file a bug report?", e);
                return Optional.empty();
            }
            Snapshot actualNextSnap;
            try {
                long actualSnapAsOfSampleSnapTime = SnapshotUtil.snapshotIdAsOfTime(actualTable, sampleNextSnap.timestampMillis());
                actualNextSnap = SnapshotUtil.snapshotAfter(actualTable, actualSnapAsOfSampleSnapTime);
            }
            catch (IllegalArgumentException | IllegalStateException | NullPointerException e) {
                // there is no snapshot in the actual table which exists after the sample was created.
                // this could happen in the case where the sample table has been updated, but no new writes
                // have occurred to the actual table after the time the sample was taken.
                // in this case, we should just use the most recent actual table snapshot
                actualNextSnap = actualTable.currentSnapshot();
            }
            TableDiff diffPrev = calculateDiff(actualTable, actualPrevSnap.snapshotId(), actualSnap.snapshotId());
            TableDiff diffNext = calculateDiff(actualTable, actualSnap.snapshotId(), actualNextSnap.snapshotId());
            if (diffPrev.compareTo(diffNext) <= 0) {
                return Optional.of(samplePrevSnap.snapshotId());
            }
            else {
                return Optional.of(sampleNextSnap.snapshotId());
            }
        });
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle, Constraint constraint)
    {
        Table icebergTable = this.icebergTable;
        if (tableHandle.getTableType() != TableType.SAMPLES && useSampleStatistics(session) &&
                SampleUtil.sampleTableExists(icebergTable, tableHandle.getSchemaName(), hdfsEnvironment, session)) {
            org.apache.iceberg.Table sampleTable = SampleUtil.getSampleTableFromActual(icebergTable, tableHandle.getSchemaName(), hdfsEnvironment, session);
            Optional<Long> sampleTableSnapshotId = calculateSnapshotToUseFromSample(icebergTable, tableHandle, sampleTable);
            LOG.debug("using sample table statistics with snapshotID " + sampleTableSnapshotId);
            icebergTable = sampleTable;
            tableHandle = new IcebergTableHandle(tableHandle.getSchemaName(), sampleTable.name(), tableHandle.getTableType(), sampleTableSnapshotId, tableHandle.getPredicate());
        }
        Optional<Long> snapshotId = tableHandle.getSnapshotId();

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
                .useSnapshot(snapshotId.get())
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

        double recordCount = summary.getRecordCount();
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(recordCount));
        result.setTotalSize(Estimate.of(summary.getSize()));
        for (IcebergColumnHandle columnHandle : idToColumnHandle.values()) {
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
