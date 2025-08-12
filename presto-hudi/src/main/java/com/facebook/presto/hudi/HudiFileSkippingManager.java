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
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static java.util.Objects.requireNonNull;

public class HudiFileSkippingManager
{
    private static final Logger log = Logger.get(HudiFileSkippingManager.class);

    private final Optional<String> specifiedQueryInstant;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTableMetadata metadataTable;

    private final Map<String, List<FileSlice>> allInputFileSlices;

    public HudiFileSkippingManager(
            List<String> partitions,
            String spillableDir,
            HoodieEngineContext engineContext,
            HoodieTableMetaClient metaClient,
            Optional<String> specifiedQueryInstant)
    {
        requireNonNull(partitions, "partitions is null");
        requireNonNull(spillableDir, "spillableDir is null");
        requireNonNull(engineContext, "engineContext is null");
        this.specifiedQueryInstant = requireNonNull(specifiedQueryInstant, "specifiedQueryInstant is null");
        this.metaClient = requireNonNull(metaClient, "metaClient is null");

        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        this.metadataTable = HoodieTableMetadata.create(engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePathV2().toString(), true);
        this.allInputFileSlices = prepareAllInputFileSlices(partitions, engineContext, metadataConfig, spillableDir);
    }

    private Map<String, List<FileSlice>> prepareAllInputFileSlices(
            List<String> partitions,
            HoodieEngineContext engineContext,
            HoodieMetadataConfig metadataConfig,
            String spillableDir)
    {
        long startTime = System.currentTimeMillis();
        HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
        Optional<HoodieInstant> latestInstant = activeTimeline.lastInstant().toJavaOptional();
        // build system view.
        SyncableFileSystemView fileSystemView = FileSystemViewManager.createViewManager(
                        engineContext,
                        FileSystemViewStorageConfig.newBuilder().withBaseStoreDir(spillableDir).build(),
                        HoodieCommonConfig.newBuilder().build(),
                        (e) -> metadataTable)
                .getFileSystemView(metaClient);
        Optional<String> queryInstant = specifiedQueryInstant.isPresent() ?
                specifiedQueryInstant : latestInstant.map(HoodieInstant::getTimestamp);

        Map<String, List<FileSlice>> allInputFileSlices = engineContext
                .mapToPair(
                        partitions,
                        partitionPath -> Pair.of(
                                partitionPath,
                                getLatestFileSlices(partitionPath, fileSystemView, queryInstant)),
                        partitions.size());

        long duration = System.currentTimeMillis() - startTime;
        log.debug("prepare query files for table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), duration);
        return allInputFileSlices;
    }

    private List<FileSlice> getLatestFileSlices(
            String partitionPath,
            SyncableFileSystemView fileSystemView,
            Optional<String> queryInstant)
    {
        return queryInstant
                .map(instant ->
                        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant.get()))
                .orElse(fileSystemView.getLatestFileSlices(partitionPath))
                .collect(Collectors.toList());
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
            log.warn(e, "failed to do data skipping for table: %s, fallback to all files scan", metaClient.getBasePathV2());
        }
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = allInputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles + 0.0d);
            log.debug("Total files: %s; candidate files after data skipping: %s; skipping percent %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent);
        }
        return candidateFileSlices;
    }

    private Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(
            Map<String, List<FileSlice>> inputFileSlices,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        // split regular column predicates
        TupleDomain<HudiColumnHandle> regularTupleDomain = HudiPredicates.from(tupleDomain).getRegularColumnPredicates();
        TupleDomain<String> regularColumnPredicates = regularTupleDomain.transform(HudiColumnHandle::getName);
        if (regularColumnPredicates.isAll() || !regularColumnPredicates.getDomains().isPresent()) {
            return inputFileSlices;
        }
        List<String> regularColumns = new ArrayList<>(regularColumnPredicates.getDomains().get().keySet());
        // get filter columns
        List<String> encodedTargetColumnNames = regularColumns
                .stream()
                .map(col -> new ColumnIndexID(col).asBase64EncodedString()).collect(Collectors.toList());
        Map<String, List<HoodieMetadataColumnStats>> statsByFileName = metadataTable.getRecordsByKeyPrefixes(
                        encodedTargetColumnNames,
                        HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, true)
                .collectAsList()
                .stream()
                .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                .map(f -> f.getData().getColumnStatMetadata().get())
                .collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

        // prune files.
        return inputFileSlices
                .entrySet()
                .stream()
                .collect(Collectors
                        .toMap(Map.Entry::getKey, entry -> entry
                                .getValue()
                                .stream()
                                .filter(fileSlice -> pruneFiles(fileSlice, statsByFileName, regularColumnPredicates, regularColumns))
                                .collect(Collectors.toList())));
    }

    private boolean pruneFiles(
            FileSlice fileSlice,
            Map<String, List<HoodieMetadataColumnStats>> statsByFileName,
            TupleDomain<String> regularColumnPredicates,
            List<String> regularColumns)
    {
        String fileSliceName = fileSlice.getBaseFile().map(BaseFile::getFileName).orElse("");
        // no stats found
        if (!statsByFileName.containsKey(fileSliceName)) {
            return true;
        }
        List<HoodieMetadataColumnStats> stats = statsByFileName.get(fileSliceName);
        return evaluateStatisticPredicate(regularColumnPredicates, stats, regularColumns);
    }

    private boolean evaluateStatisticPredicate(
            TupleDomain<String> regularColumnPredicates,
            List<HoodieMetadataColumnStats> stats,
            List<String> regularColumns)
    {
        if (regularColumnPredicates.isNone() || !regularColumnPredicates.getDomains().isPresent()) {
            return true;
        }
        for (String regularColumn : regularColumns) {
            Domain columnPredicate = regularColumnPredicates.getDomains().get().get(regularColumn);
            Optional<HoodieMetadataColumnStats> currentColumnStats = stats
                    .stream().filter(s -> s.getColumnName().equals(regularColumn)).findFirst();
            if (currentColumnStats.isPresent()) {
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
        if (!(statistics.getMinValue() instanceof org.apache.hudi.org.apache.avro.generic.GenericRecord) ||
                !(statistics.getMaxValue() instanceof org.apache.hudi.org.apache.avro.generic.GenericRecord)) {
            return Domain.all(type);
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

            if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(DATE))) {
                long minValue = TupleDomainParquetPredicate.asLong(minimum);
                long maxValue = TupleDomainParquetPredicate.asLong(maximum);
                if (isStatisticsOverflow(type, minValue, maxValue)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (type.equals(REAL)) {
                Float minValue = (Float) minimum;
                Float maxValue = (Float) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, (long) floatToRawIntBits(minValue), (long) floatToRawIntBits(maxValue), hasNullValue);
            }

            if (type.equals(DOUBLE)) {
                Double minValue = (Double) minimum;
                Double maxValue = (Double) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (isVarcharType(type)) {
                Slice min = Slices.utf8Slice((String) minimum);
                Slice max = Slices.utf8Slice((String) maximum);
                return ofMinMax(type, min, max, hasNullValue);
            }
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        catch (Exception e) {
            log.warn("failed to create Domain for column: %s which type is: %s", colName, type.toString());
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }

    private static Domain ofMinMax(Type type, Object min, Object max, boolean hasNullValue)
    {
        Range range = Range.range(type, min, true, max, true);
        ValueSet vs = ValueSet.ofRanges(ImmutableList.of(range));
        return Domain.create(vs, hasNullValue);
    }
}
