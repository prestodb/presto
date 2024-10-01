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
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.iceberg.statistics.StatisticsFileCacheKey;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getStatisticSnapshotRecordDifferenceWeight;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.Partition.toMap;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateAndSetTableSize;
import static com.facebook.presto.iceberg.util.StatisticsUtil.formatIdentifier;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Long.parseLong;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;

public class TableStatisticsMaker
{
    private static final Logger log = Logger.get(TableStatisticsMaker.class);
    private static final String ICEBERG_THETA_SKETCH_BLOB_TYPE_ID = "apache-datasketches-theta-v1";
    private static final String ICEBERG_DATA_SIZE_BLOB_TYPE_ID = "presto-sum-data-size-bytes-v1";
    private static final String ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY = "ndv";
    private static final String ICEBERG_DATA_SIZE_BLOB_PROPERTY_KEY = "data_size";
    private final Table icebergTable;
    private final ConnectorSession session;
    private final TypeManager typeManager;

    private static final String STATISITCS_CACHE_METRIC_FILE_SIZE_FORMAT = "StatisticsFileCache/PuffinFileSize/%s/%s";
    private static final String STATISITCS_CACHE_METRIC_FILE_COLUMN_COUNT_FORMAT = "StatisticsFileCache/ColumnCount/%s/%s";
    private static final String STATISITCS_CACHE_METRIC_PARTIAL_MISS_FORMAT = "StatisticsFileCache/PartialMiss/%s/%s";

    private TableStatisticsMaker(Table icebergTable, ConnectorSession session, TypeManager typeManager)
    {
        this.icebergTable = icebergTable;
        this.session = session;
        this.typeManager = typeManager;
    }

    private static final Map<ColumnStatisticType, PuffinBlobGenerator> puffinStatWriters = ImmutableMap.<ColumnStatisticType, PuffinBlobGenerator>builder()
            .put(NUMBER_OF_DISTINCT_VALUES, TableStatisticsMaker::generateNDVBlob)
            .put(TOTAL_SIZE_IN_BYTES, TableStatisticsMaker::generateStatSizeBlob)
            .build();

    private static final Map<String, PuffinBlobReader> puffinStatReaders = ImmutableMap.<String, PuffinBlobReader>builder()
            .put(ICEBERG_THETA_SKETCH_BLOB_TYPE_ID, TableStatisticsMaker::readNDVBlob)
            .put(ICEBERG_DATA_SIZE_BLOB_TYPE_ID, TableStatisticsMaker::readDataSizeBlob)
            .build();

    public static TableStatistics getTableStatistics(
            ConnectorSession session,
            TypeManager typeManager,
            StatisticsFileCache statisticsFileCache,
            Optional<TupleDomain<IcebergColumnHandle>> currentPredicate,
            Constraint constraint,
            IcebergTableHandle tableHandle,
            Table icebergTable,
            List<IcebergColumnHandle> columns)
    {
        return new TableStatisticsMaker(icebergTable, session, typeManager).makeTableStatistics(statisticsFileCache, tableHandle, currentPredicate, constraint, columns);
    }

    private TableStatistics makeTableStatistics(StatisticsFileCache statisticsFileCache,
            IcebergTableHandle tableHandle,
            Optional<TupleDomain<IcebergColumnHandle>> currentPredicate,
            Constraint constraint,
            List<IcebergColumnHandle> selectedColumns)
    {
        if (!tableHandle.getIcebergTableName().getSnapshotId().isPresent() || constraint.getSummary().isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        TupleDomain<IcebergColumnHandle> intersection = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast);
        if (currentPredicate.isPresent()) {
            intersection.intersect(currentPredicate.get());
        }

        if (intersection.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .setConfidenceLevel(HIGH)
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

        Partition summary;
        if (tableHandle.getIcebergTableName().getTableType() == IcebergTableType.EQUALITY_DELETES) {
            summary = getEqualityDeleteTableSummary(tableHandle, intersection, idToTypeMapping, nonPartitionPrimitiveColumns, partitionFields);
        }
        else {
            summary = getDataTableSummary(tableHandle, selectedColumns, intersection, idToTypeMapping, nonPartitionPrimitiveColumns, partitionFields);
        }

        if (summary == null) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }
        // the total record count for the whole table
        Optional<Long> totalRecordCount = Optional.of(intersection)
                .filter(domain -> !domain.isAll())
                .map(domain -> getDataTableSummary(tableHandle, ImmutableList.of(), TupleDomain.all(), idToTypeMapping, nonPartitionPrimitiveColumns, partitionFields).getRecordCount());

        double recordCount = summary.getRecordCount();
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(recordCount));

        // transformValues returns a view. We need to make a copy of the map in order to update
        Map<Integer, ColumnStatistics.Builder> tableStats = ImmutableMap.copyOf(
                Maps.transformValues(getClosestStatisticsFileForSnapshot(tableHandle)
                                .map(file -> loadStatisticsFile(tableHandle, file, statisticsFileCache, selectedColumns.stream()
                                        .map(IcebergColumnHandle::getId)
                                        .collect(Collectors.toList())))
                                .orElseGet(Collections::emptyMap),
                        ColumnStatistics::buildFrom));
        // scale all NDV values loaded from puffin files based on row count
        totalRecordCount.ifPresent(fullTableRecordCount -> tableStats.forEach((id, stat) ->
                stat.setDistinctValuesCount(stat.getDistinctValuesCount().map(value -> value * recordCount / fullTableRecordCount))));

        for (IcebergColumnHandle columnHandle : selectedColumns) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = tableStats.getOrDefault(fieldId, ColumnStatistics.builder());
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }

            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                columnBuilder.setRange(Optional.of(new DoubleRange(((Number) min).doubleValue(), ((Number) max).doubleValue())));
            }
            result.setColumnStatistics(columnHandle, columnBuilder.build());
        }
        return calculateAndSetTableSize(result).build();
    }

    private Partition getDataTableSummary(IcebergTableHandle tableHandle,
            List<IcebergColumnHandle> selectedColumns,
            TupleDomain<IcebergColumnHandle> intersection,
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<Types.NestedField> nonPartitionPrimitiveColumns,
            List<PartitionField> partitionFields)
    {
        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .select(selectedColumns.stream().map(IcebergColumnHandle::getName).collect(Collectors.toList()))
                .useSnapshot(tableHandle.getIcebergTableName().getSnapshotId().get())
                .includeColumnStats();

        CloseableIterable<ContentFile<?>> files = CloseableIterable.transform(tableScan.planFiles(), ContentScanTask::file);
        return getSummaryFromFiles(files, idToTypeMapping, nonPartitionPrimitiveColumns, partitionFields);
    }

    private Partition getEqualityDeleteTableSummary(IcebergTableHandle tableHandle,
            TupleDomain<IcebergColumnHandle> intersection,
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<Types.NestedField> nonPartitionPrimitiveColumns,
            List<PartitionField> partitionFields)
    {
        CloseableIterable<DeleteFile> deleteFiles = IcebergUtil.getDeleteFiles(icebergTable,
                tableHandle.getIcebergTableName().getSnapshotId().get(),
                intersection,
                tableHandle.getPartitionSpecId(),
                tableHandle.getEqualityFieldIds());
        CloseableIterable<ContentFile<?>> files = CloseableIterable.transform(deleteFiles, deleteFile -> deleteFile);
        return getSummaryFromFiles(files, idToTypeMapping, nonPartitionPrimitiveColumns, partitionFields);
    }

    private Partition getSummaryFromFiles(CloseableIterable<ContentFile<?>> files,
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<Types.NestedField> nonPartitionPrimitiveColumns,
            List<PartitionField> partitionFields)
    {
        Partition summary = null;
        try (CloseableIterable<ContentFile<?>> filesHolder = files) {
            for (ContentFile<?> contentFile : filesHolder) {
                if (summary == null) {
                    summary = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            contentFile.partition(),
                            contentFile.recordCount(),
                            contentFile.fileSizeInBytes(),
                            toMap(idToTypeMapping, contentFile.lowerBounds()),
                            toMap(idToTypeMapping, contentFile.upperBounds()),
                            contentFile.nullValueCounts(),
                            new HashMap<>());
                }
                else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(contentFile.recordCount());
                    summary.incrementSize(contentFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, toMap(idToTypeMapping, contentFile.lowerBounds()), contentFile.nullValueCounts(), contentFile.recordCount());
                    updateSummaryMax(summary, partitionFields, toMap(idToTypeMapping, contentFile.upperBounds()), contentFile.nullValueCounts(), contentFile.recordCount());
                    summary.updateNullCount(contentFile.nullValueCounts());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return summary;
    }

    public static void writeTableStatistics(NodeVersion nodeVersion, TypeManager typeManager, IcebergTableHandle tableHandle, Table icebergTable, ConnectorSession session, Collection<ComputedStatistics> computedStatistics)
    {
        new TableStatisticsMaker(icebergTable, session, typeManager).writeTableStatistics(nodeVersion, tableHandle, computedStatistics);
    }

    private void writeTableStatistics(NodeVersion nodeVersion, IcebergTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Snapshot snapshot = tableHandle.getIcebergTableName().getSnapshotId().map(icebergTable::snapshot).orElseGet(icebergTable::currentSnapshot);
        if (snapshot == null) {
            // this may occur if the table has not been written to.
            return;
        }
        try (FileIO io = icebergTable.io()) {
            String path = ((HasTableOperations) icebergTable).operations().metadataFileLocation(format("%s-%s.stats", session.getQueryId(), randomUUID()));
            OutputFile outputFile = io.newOutputFile(path);
            try (PuffinWriter writer = Puffin.write(outputFile)
                    .createdBy("presto-" + nodeVersion)
                    .build()) {
                computedStatistics.stream()
                        .map(ComputedStatistics::getColumnStatistics)
                        .filter(Objects::nonNull)
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                        .forEach((key, value) -> {
                            Optional.ofNullable(puffinStatWriters.get(key.getStatisticType()))
                                    .ifPresent(generator -> {
                                        writer.add(generator.generate(key, value, icebergTable, snapshot));
                                    });
                        });
                writer.finish();
                icebergTable.updateStatistics().setStatistics(
                                snapshot.snapshotId(),
                                new GenericStatisticsFile(
                                        snapshot.snapshotId(),
                                        path,
                                        writer.fileSize(),
                                        writer.footerSize(),
                                        writer.writtenBlobsMetadata().stream()
                                                .map(GenericBlobMetadata::from)
                                                .collect(toImmutableList())))
                        .commit();
            }
            catch (IOException e) {
                log.warn(e, "failed to write table statistics file");
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "failed to write statistics file", e);
            }
        }
    }

    @FunctionalInterface
    private interface PuffinBlobGenerator
    {
        Blob generate(ColumnStatisticMetadata metadata, Block value, Table icebergTable, Snapshot snapshot);
    }

    @FunctionalInterface
    private interface PuffinBlobReader
    {
        /**
         * Reads the stats from the blob and then updates the stats builder argument.
         */
        void read(BlobMetadata metadata, ByteBuffer blob, ColumnStatistics.Builder stats);
    }

    private static Blob generateNDVBlob(ColumnStatisticMetadata metadata, Block value, Table icebergTable, Snapshot snapshot)
    {
        int id = getFieldId(metadata, icebergTable);
        ByteBuffer raw = VARBINARY.getSlice(value, 0).toByteBuffer();
        CompactSketch sketch = CompactSketch.wrap(Memory.wrap(raw, ByteOrder.nativeOrder()));
        return new Blob(
                ICEBERG_THETA_SKETCH_BLOB_TYPE_ID,
                ImmutableList.of(id),
                snapshot.snapshotId(),
                snapshot.sequenceNumber(),
                raw,
                null,
                ImmutableMap.of(ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY, Long.toString((long) sketch.getEstimate())));
    }

    private static Blob generateStatSizeBlob(ColumnStatisticMetadata metadata, Block value, Table icebergTable, Snapshot snapshot)
    {
        int id = getFieldId(metadata, icebergTable);
        long size = BIGINT.getLong(value, 0);
        return new Blob(
                ICEBERG_DATA_SIZE_BLOB_TYPE_ID,
                ImmutableList.of(id),
                snapshot.snapshotId(),
                snapshot.sequenceNumber(),
                ByteBuffer.allocate(0), // empty bytebuffer since the value is just stored on the blob properties
                null,
                ImmutableMap.of(ICEBERG_DATA_SIZE_BLOB_PROPERTY_KEY, Long.toString(size)));
    }

    private static void readNDVBlob(BlobMetadata metadata, ByteBuffer blob, ColumnStatistics.Builder statistics)
    {
        Optional.ofNullable(metadata.properties().get(ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY))
                .ifPresent(ndvProp -> {
                    try {
                        long ndv = parseLong(ndvProp);
                        statistics.setDistinctValuesCount(Estimate.of(ndv));
                    }
                    catch (NumberFormatException e) {
                        statistics.setDistinctValuesCount(Estimate.unknown());
                        log.warn("bad long value when parsing NDVs for statistics file blob %s. bad value: %d", metadata.type(), ndvProp);
                    }
                });
    }

    private static void readDataSizeBlob(BlobMetadata metadata, ByteBuffer blob, ColumnStatistics.Builder statistics)
    {
        Optional.ofNullable(metadata.properties().get(ICEBERG_DATA_SIZE_BLOB_PROPERTY_KEY))
                .ifPresent(sizeProp -> {
                    try {
                        long size = parseLong(sizeProp);
                        statistics.setDataSize(Estimate.of(size));
                    }
                    catch (NumberFormatException e) {
                        statistics.setDataSize(Estimate.unknown());
                        log.warn("bad long value when parsing data size from statistics file blob %s. bad value: %d", metadata.type(), sizeProp);
                    }
                });
    }

    private static int getFieldId(ColumnStatisticMetadata metadata, Table icebergTable)
    {
        return Optional.ofNullable(icebergTable.schema().findField(metadata.getColumnName())).map(Types.NestedField::fieldId)
                .orElseThrow(() -> {
                    log.warn("failed to find column name %s in schema of table %s", metadata.getColumnName(), icebergTable.name());
                    return new PrestoException(ICEBERG_INVALID_METADATA, format("failed to find column name %s in schema of table %s", metadata.getColumnName(), icebergTable.name()));
                });
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

    private Optional<StatisticsFile> getClosestStatisticsFileForSnapshot(IcebergTableHandle handle)
    {
        Snapshot target = handle.getIcebergTableName().getSnapshotId().map(icebergTable::snapshot).orElseGet(icebergTable::currentSnapshot);
        return icebergTable.statisticsFiles()
                .stream()
                .min((first, second) -> {
                    if (first == second) {
                        return 0;
                    }
                    if (icebergTable.snapshot(first.snapshotId()) == null) {
                        return 1;
                    }
                    if (icebergTable.snapshot(second.snapshotId()) == null) {
                        return -1;
                    }
                    Snapshot firstSnap = icebergTable.snapshot(first.snapshotId());
                    Snapshot secondSnap = icebergTable.snapshot(second.snapshotId());
                    long firstDiff = abs(target.timestampMillis() - firstSnap.timestampMillis());
                    long secondDiff = abs(target.timestampMillis() - secondSnap.timestampMillis());

                    // check if total-record exists
                    Optional<Long> targetTotalRecords = Optional.ofNullable(target.summary().get(TOTAL_RECORDS_PROP)).map(Long::parseLong);
                    Optional<Long> firstTotalRecords = Optional.ofNullable(firstSnap.summary().get(TOTAL_RECORDS_PROP))
                            .map(Long::parseLong);
                    Optional<Long> secondTotalRecords = Optional.ofNullable(secondSnap.summary().get(TOTAL_RECORDS_PROP))
                            .map(Long::parseLong);

                    if (targetTotalRecords.isPresent() && firstTotalRecords.isPresent() && secondTotalRecords.isPresent()) {
                        long targetTotal = targetTotalRecords.get();
                        double weight = getStatisticSnapshotRecordDifferenceWeight(session);
                        firstDiff += (long) (weight * abs(firstTotalRecords.get() - targetTotal));
                        secondDiff += (long) (weight * abs(secondTotalRecords.get() - targetTotal));
                    }

                    return Long.compare(firstDiff, secondDiff);
                });
    }

    /**
     * Builds a map of field ID to ColumnStatistics for a particular {@link StatisticsFile}.
     */
    private Map<Integer, ColumnStatistics> loadStatisticsFile(IcebergTableHandle tableHandle, StatisticsFile file, StatisticsFileCache statisticsFileCache, List<Integer> columnIds)
    {
        // first, try to load all stats from the cache. If the map doesn't contain all keys, load the missing
        // stats into the cache.
        Map<Integer, ColumnStatistics> cachedStats = columnIds.stream()
                .map(id -> Pair.of(id, statisticsFileCache.getIfPresent(new StatisticsFileCacheKey(file, id))))
                .filter(pair -> pair.second() != null)
                .collect(toImmutableMap(Pair::first, Pair::second));
        Set<Integer> missingStats = columnIds.stream().filter(id -> !cachedStats.containsKey(id)).collect(toImmutableSet());
        if (missingStats.isEmpty()) {
            return cachedStats;
        }
        if (!cachedStats.isEmpty()) {
            session.getRuntimeStats().addMetricValue(
                    String.format(STATISITCS_CACHE_METRIC_PARTIAL_MISS_FORMAT,
                            tableHandle.getSchemaTableName(),
                            file.path()),
                    RuntimeUnit.NONE, 1);
        }

        String fileSizeMetricName = String.format(STATISITCS_CACHE_METRIC_FILE_SIZE_FORMAT,
                tableHandle.getSchemaTableName(),
                file.path());
        if (session.getRuntimeStats().getMetric(fileSizeMetricName) == null) {
            long fileSize = file.fileFooterSizeInBytes();
            session.getRuntimeStats().addMetricValue(fileSizeMetricName, RuntimeUnit.NONE, fileSize);
            statisticsFileCache.recordFileSize(fileSize);
        }
        String columnCountMetricName = String.format(STATISITCS_CACHE_METRIC_FILE_COLUMN_COUNT_FORMAT,
                tableHandle.getSchemaTableName(),
                file.path());
        if (session.getRuntimeStats().getMetric(columnCountMetricName) == null) {
            long columnCount = file.blobMetadata().size();
            session.getRuntimeStats().addMetricValue(columnCountMetricName, RuntimeUnit.NONE, columnCount);
            statisticsFileCache.recordColumnCount(columnCount);
        }
        Map<Integer, ColumnStatistics.Builder> result = new HashMap<>();
        try (FileIO io = icebergTable.io()) {
            InputFile inputFile = io.newInputFile(file.path());
            try (PuffinReader reader = Puffin.read(inputFile).build()) {
                for (Pair<BlobMetadata, ByteBuffer> data : reader.readAll(reader.fileMetadata().blobs())) {
                    BlobMetadata metadata = data.first();
                    ByteBuffer blob = data.second();
                    Integer field = getOnlyElement(metadata.inputFields());
                    if (!missingStats.contains(field)) {
                        continue;
                    }

                    Optional.ofNullable(puffinStatReaders.get(metadata.type()))
                            .ifPresent(statReader -> {
                                result.compute(field, (key, value) -> {
                                    if (value == null) {
                                        value = ColumnStatistics.builder();
                                    }
                                    statReader.read(metadata, blob, value);
                                    return value;
                                });
                            });
                }
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "failed to read statistics file at " + file.path(), e);
            }
        }
        Map<Integer, ColumnStatistics> computedResults = new HashMap<>(Maps.transformValues(result, ColumnStatistics.Builder::build));
        missingStats.stream()
                .filter(id -> !computedResults.containsKey(id))
                .forEach(id -> computedResults.put(id, ColumnStatistics.empty()));
        ImmutableMap.Builder<Integer, ColumnStatistics> finalResult = ImmutableMap.builder();
        computedResults.forEach((key, value) -> {
            // stats for a particular column may not appear in a file. Add a cache entry to
            // denote that this file has been read for a particular column to avoid reading the
            // file again
            statisticsFileCache.put(new StatisticsFileCacheKey(file, key), value);
            finalResult.put(key, value);
        });
        return finalResult.build();
    }

    public static List<ColumnStatisticMetadata> getSupportedColumnStatistics(String columnName, com.facebook.presto.common.type.Type type)
    {
        ImmutableList.Builder<ColumnStatisticMetadata> supportedStatistics = ImmutableList.builder();
        // all types which support being passed to the sketch_theta function
        if (isNumericType(type) || type.equals(DATE) || isVarcharType(type) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            supportedStatistics.add(NUMBER_OF_DISTINCT_VALUES.getColumnStatisticMetadataWithCustomFunction(
                    columnName, format("RETURN sketch_theta(%s)", formatIdentifier(columnName)), ImmutableList.of(columnName)));
        }

        if (!(type instanceof FixedWidthType)) {
            supportedStatistics.add(TOTAL_SIZE_IN_BYTES.getColumnStatisticMetadata(columnName));
        }

        return supportedStatistics.build();
    }
}
