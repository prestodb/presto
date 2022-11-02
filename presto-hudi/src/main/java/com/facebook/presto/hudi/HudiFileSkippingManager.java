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

package com.facebook.presto.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static java.lang.Float.floatToRawIntBits;

/**
 * MetaDataTable based filesManager.
 * list Hudi Table contents based on the
 *
 * <ul>
 *   <li>Table type (MOR, COW)</li>
 *   <li>Query type (snapshot, read_optimized, incremental)</li>
 *   <li>Query instant/range</li>
 * </ul>
 *
 * Support dataSkipping.
 */
public class HudiFileSkippingManager
{
    private static final Logger log = Logger.get(HudiFileSkippingManager.class);

    private final HoodieTableQueryType queryType;
    private final Option<String> specifiedQueryInstant;
    private final HoodieTableMetaClient metaClient;
    private final HoodieEngineContext engineContext;
    private final String spillableDir;
    private final HoodieTableType tableType;
    protected final HoodieMetadataConfig metadataConfig;
    private final HoodieTableMetadata metadataTable;

    protected transient volatile Map<String, List<FileSlice>> allInputFileSlices;

    private transient volatile HoodieTableFileSystemView fileSystemView;

    public HudiFileSkippingManager(
            List<String> partitions,
            String spillableDir,
            HoodieEngineContext engineContext,
            HoodieTableMetaClient metaClient,
            HoodieTableQueryType queryType,
            Option<String> specifiedQueryInstant)
    {
        this.queryType = queryType;
        this.specifiedQueryInstant = specifiedQueryInstant;
        this.metaClient = metaClient;
        this.engineContext = engineContext;
        this.spillableDir = spillableDir;
        this.metadataConfig = HoodieMetadataConfig.newBuilder().withMetadataIndexBloomFilter(true).enable(true).build();
        this.tableType = metaClient.getTableConfig().getTableType();
        this.metadataTable = HoodieTableMetadata
                .create(engineContext, this.metadataConfig, metaClient.getBasePathV2().toString(), spillableDir, true);
        prepareAllInputFileSlices(partitions);
    }

    private void prepareAllInputFileSlices(List<String> partitions)
    {
        long startTime = System.currentTimeMillis();
        Path tablePath = metaClient.getBasePathV2();
        List<String> fullPartitionPath = partitions.stream().map(p ->
                p.isEmpty() ? tablePath.toString() : new Path(tablePath, p).toString()).collect(Collectors.toList());
        Map<String, FileStatus[]> fetchedPartitionToFiles = FSUtils.getFilesInPartitions(
                engineContext,
                metadataConfig,
                metaClient.getBasePathV2().toString(),
                fullPartitionPath.toArray(new String[0]),
                spillableDir);
        FileStatus[] allFiles = fetchedPartitionToFiles.values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);

        HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
        Option<HoodieInstant> latestInstant = activeTimeline.lastInstant();
        // build system view.
        fileSystemView = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles);
        Option<String> queryInstant = specifiedQueryInstant.or(() -> latestInstant.map(HoodieInstant::getTimestamp));
        if (tableType.equals(HoodieTableType.MERGE_ON_READ) && queryType.equals(HoodieTableQueryType.SNAPSHOT)) {
            allInputFileSlices = partitions.stream().collect(Collectors.toMap(Function.identity(),
                    partitionPath ->
                            queryInstant.map(instant ->
                                    fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant.get())
                                            .collect(Collectors.toList()))
                                    .orElse(Collections.emptyList())));
        }
        else {
            allInputFileSlices = partitions.stream().collect(Collectors.toMap(Function.identity(), partition ->
                    queryInstant.map(instant ->
                            fileSystemView.getLatestFileSlicesBeforeOrOn(partition, instant, true))
                            .orElse(fileSystemView.getLatestFileSlices(partition))
                            .collect(Collectors.toList())));
        }

        long duration = System.currentTimeMillis() - startTime;

        log.info(String.format("prepare query files for table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), duration));
    }

    public static HoodieTableQueryType getQueryType(ConnectorSession session, String inputFormat)
    {
        // TODO support incremental query
        switch (inputFormat) {
            case "org.apache.hudi.hadoop.HoodieParquetInputFormat":
            case "com.uber.hoodie.hadoop.HoodieInputFormat":
                // cow table/ mor ro table
                return HoodieTableQueryType.READ_OPTIMIZED;
            case "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat":
            case "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat":
                // mor rt table
                return HoodieTableQueryType.SNAPSHOT;
            default:
                throw new IllegalArgumentException(String.format("failed to infer query type for current inputFormat: %s", inputFormat));
        }
    }

    public Map<String, List<FileSlice>> listQueryFiles(TupleDomain<ColumnHandle> tupleDomain)
    {
        // do file skipping by MetadataTable
        Map<String, List<FileSlice>> candidateFileSlices = allInputFileSlices;
        try {
            if (!tupleDomain.isAll()) {
                candidateFileSlices = lookupCandidateFilesInMetadataTable(candidateFileSlices, tupleDomain);
            }
        }
        catch (Exception e) {
            // Should not throw exception, just log this Exception.
            log.warn(String.format("failed to do data skipping for table: %s, fallback to all files scan", metaClient.getBasePathV2().toString()), e);
            candidateFileSlices = allInputFileSlices;
        }
        int candidateFileSize = candidateFileSlices.entrySet().stream().map(entry -> entry.getValue().size()).reduce(0, (n1, n2) -> n1 + n2);
        int totalFiles = allInputFileSlices.entrySet().stream().map(entry -> entry.getValue().size()).reduce(0, (n1, n2) -> n1 + n2);
        double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles + 0.0d);
        log.info(String.format("Total files: %s; candidate files after data skipping: %s; skipping percent %s",
                totalFiles, candidateFileSize, skippingPercent));
        return candidateFileSlices;
    }

    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(Map<String, List<FileSlice>> inputFileSlices, TupleDomain<ColumnHandle> tupleDomain)
    {
        // split regular column predicates
        TupleDomain<HudiColumnHandle> regularTupleDomain = HudiPredicates.from(tupleDomain).getRegularColumnPredicates();
        TupleDomain<String> regularColumnPredicates = regularTupleDomain.transform(HudiColumnHandle::getName);
        if (regularColumnPredicates.isAll()) {
            return inputFileSlices;
        }
        List<String> regularColumns = regularColumnPredicates.getDomains().get().entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        // get filter columns
        List<String> encodedTargetColumnNames = regularColumns.stream().map(col -> new ColumnIndexID(col).asBase64EncodedString()).collect(Collectors.toList());
        Map<String, List<HoodieMetadataColumnStats>> statsByFileName = metadataTable.getRecordsByKeyPrefixes(
                encodedTargetColumnNames,
                HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, true)
                .collectAsList()
                .stream()
                .filter(f -> f.getData().getColumnStatMetadata()
                        .isPresent())
                .map(f -> f.getData().getColumnStatMetadata().get()).collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

        // prune files.
        return inputFileSlices.entrySet().parallelStream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
            return entry.getValue().stream().filter(fileSlice -> {
                String fileSliceName = fileSlice.getBaseFile().map(BaseFile::getFileName).orElse("");
                // no stats found
                if (!statsByFileName.containsKey(fileSliceName)) {
                    return true;
                }
                List<HoodieMetadataColumnStats> stats = statsByFileName.get(fileSliceName);
                if (!evaluateStatisticPredicate(regularColumnPredicates, stats, regularColumns)) {
                    return false;
                }
                return true;
            }).collect(Collectors.toList());
        }));
    }

    private boolean evaluateStatisticPredicate(TupleDomain<String> regularColumnPredicates, List<HoodieMetadataColumnStats> stats, List<String> regularColumns)
    {
        if (regularColumnPredicates.isNone()) {
            return true;
        }
        for (String regularColumn : regularColumns) {
            Domain columnPredicate = regularColumnPredicates.getDomains().get().get(regularColumn);
            Optional<HoodieMetadataColumnStats> currentColumnStats = stats.stream().filter(s -> s.getColumnName().equals(regularColumn)).findFirst();
            if (!currentColumnStats.isPresent()) {
                // no stats for column
            }
            else {
                Domain domain = getDomain(regularColumn, columnPredicate.getType(), currentColumnStats.get());
                if (columnPredicate.intersect(domain).isNone()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Domain getDomain(String colName, Type type, HoodieMetadataColumnStats statistics)
    {
        if (statistics == null) {
            return Domain.all(type);
        }
        boolean hasNullValue = statistics.getNullCount() != 0L;
        boolean hasNonNullValue = statistics.getValueCount() - statistics.getNullCount() > 0;
        if (!hasNonNullValue || statistics.getMaxValue() == null || statistics.getMinValue() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        return getDomain(colName, type, ((org.apache.hudi.org.apache.avro.generic.GenericRecord) statistics.getMinValue()).get(0),
                ((org.apache.hudi.org.apache.avro.generic.GenericRecord) statistics.getMaxValue()).get(0), hasNullValue);
    }

    /**
     * Get a domain for the ranges defined by each pair of elements from {@code minimums} and {@code maximums}.
     * Both arrays must have the same length.
     */
    private static Domain getDomain(String colName, Type type, Object minimum, Object maximum, boolean hasNullValue)
    {
        List<Range> ranges = new ArrayList<>();
        try {
            if (type.equals(BOOLEAN)) {
                boolean hasTrueValue = (boolean) minimum || (boolean) maximum;
                boolean hasFalseValue = !(boolean) minimum || !(boolean) maximum;
                if (hasTrueValue && hasFalseValue) {
                    return Domain.all(type);
                }
                if (hasTrueValue) {
                    return Domain.create(ValueSet.of(type, true), hasNullValue);
                }
                if (hasFalseValue) {
                    return Domain.create(ValueSet.of(type, false), hasNullValue);
                }
                // No other case, since all null case is handled earlier.
            }

            if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER))) {
                long minValue = TupleDomainParquetPredicate.asLong(minimum);
                long maxValue = TupleDomainParquetPredicate.asLong(maximum);
                if (isStatisticsOverflow(type, minValue, maxValue)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, minValue, true, maxValue, true));
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }

            if (type.equals(REAL)) {
                Float minValue = (Float) minimum;
                Float maxValue = (Float) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, (long) floatToRawIntBits(minValue), true, (long) floatToRawIntBits(maxValue), true));
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }

            if (type.equals(DOUBLE)) {
                Double minValue = (Double) minimum;
                Double maxValue = (Double) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, minValue, true, maxValue, true));
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }

            if (isVarcharType(type)) {
                Slice min = Slices.utf8Slice((String) minimum);
                Slice max = Slices.utf8Slice((String) maximum);
                ranges.add(Range.range(type, min, true, max, true));
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }

            if (type.equals(DATE)) {
                long min = TupleDomainParquetPredicate.asLong(minimum);
                long max = TupleDomainParquetPredicate.asLong(maximum);
                if (isStatisticsOverflow(type, min, max)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, min, true, max, true));
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        catch (Exception e) {
            log.warn(String.format("failed to create Domain for column: %s which type is: %s", colName, type.toString()));
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }
}
