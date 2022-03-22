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

package com.facebook.presto.hive.statistics;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.metastore.DateStatistics;
import com.facebook.presto.hive.metastore.DecimalStatistics;
import com.facebook.presto.hive.metastore.DoubleStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.IntegerStatistics;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.getPartitionStatisticsSampleSize;
import static com.facebook.presto.hive.HiveSessionProperties.isIgnoreCorruptedStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isStatisticsEnabled;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.hash.Hashing.murmur3_128;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.parseDouble;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class MetastoreHiveStatisticsProvider
        implements HiveStatisticsProvider
{
    private static final Logger log = Logger.get(MetastoreHiveStatisticsProvider.class);

    private final PartitionsStatisticsProvider statisticsProvider;

    public MetastoreHiveStatisticsProvider(SemiTransactionalHiveMetastore metastore)
    {
        requireNonNull(metastore, "metastore is null");
        this.statisticsProvider = (session, table, hivePartitions) -> getPartitionsStatistics(session, metastore, table, hivePartitions);
    }

    @VisibleForTesting
    MetastoreHiveStatisticsProvider(PartitionsStatisticsProvider statisticsProvider)
    {
        this.statisticsProvider = requireNonNull(statisticsProvider, "statisticsProvider is null");
    }

    private static Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SemiTransactionalHiveMetastore metastore, SchemaTableName table, List<HivePartition> hivePartitions)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableMap.of();
        }
        boolean unpartitioned = hivePartitions.stream().anyMatch(partition -> partition.getPartitionId().equals(UNPARTITIONED_ID));
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session));
        if (unpartitioned) {
            checkArgument(hivePartitions.size() == 1, "expected only one hive partition");
            return ImmutableMap.of(UNPARTITIONED_ID, metastore.getTableStatistics(metastoreContext, table.getSchemaName(), table.getTableName()));
        }
        Set<String> partitionNames = hivePartitions.stream()
                .map(HivePartition::getPartitionId)
                .collect(toImmutableSet());
        return metastore.getPartitionStatistics(metastoreContext, table.getSchemaName(), table.getTableName(), partitionNames);
    }

    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            SchemaTableName table,
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        if (partitions.isEmpty()) {
            return createZeroStatistics(columns, columnTypes);
        }
        int sampleSize = getPartitionStatisticsSampleSize(session);
        List<HivePartition> partitionsSample = getPartitionsSample(partitions, sampleSize);
        try {
            Map<String, PartitionStatistics> statisticsSample = statisticsProvider.getPartitionsStatistics(session, table, partitionsSample);
            validatePartitionStatistics(table, statisticsSample);
            return getTableStatistics(columns, columnTypes, partitions, statisticsSample);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode()) && isIgnoreCorruptedStatistics(session)) {
                log.error(e);
                return TableStatistics.empty();
            }
            throw e;
        }
    }

    private TableStatistics createZeroStatistics(Map<String, ColumnHandle> columns, Map<String, Type> columnTypes)
    {
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(0));
        result.setTotalSize(Estimate.of(0));
        columns.forEach((columnName, columnHandle) -> {
            Type columnType = columnTypes.get(columnName);
            verify(columnType != null, "columnType is missing for column: %s", columnName);
            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            columnStatistics.setNullsFraction(Estimate.of(0));
            columnStatistics.setDistinctValuesCount(Estimate.of(0));
            if (hasDataSize(columnType)) {
                columnStatistics.setDataSize(Estimate.of(0));
            }
            result.setColumnStatistics(columnHandle, columnStatistics.build());
        });
        return result.build();
    }

    @VisibleForTesting
    static List<HivePartition> getPartitionsSample(List<HivePartition> partitions, int sampleSize)
    {
        checkArgument(sampleSize > 0, "sampleSize is expected to be greater than zero");

        if (partitions.size() <= sampleSize) {
            return partitions;
        }

        List<HivePartition> result = new ArrayList<>();

        int samplesLeft = sampleSize;

        HivePartition min = partitions.get(0);
        HivePartition max = partitions.get(0);
        for (HivePartition partition : partitions) {
            if (partition.getPartitionId().compareTo(min.getPartitionId()) < 0) {
                min = partition;
            }
            else if (partition.getPartitionId().compareTo(max.getPartitionId()) > 0) {
                max = partition;
            }
        }

        result.add(min);
        samplesLeft--;
        if (samplesLeft > 0) {
            result.add(max);
            samplesLeft--;
        }

        if (samplesLeft > 0) {
            HashFunction hashFunction = murmur3_128();
            Comparator<Map.Entry<HivePartition, Long>> hashComparator = Comparator
                    .<Map.Entry<HivePartition, Long>, Long>comparing(Map.Entry::getValue)
                    .thenComparing(entry -> entry.getKey().getPartitionId());
            partitions.stream()
                    .filter(partition -> !result.contains(partition))
                    .map(partition -> immutableEntry(partition, hashFunction.hashUnencodedChars(partition.getPartitionId()).asLong()))
                    .sorted(hashComparator)
                    .limit(samplesLeft)
                    .forEachOrdered(entry -> result.add(entry.getKey()));
        }

        return unmodifiableList(result);
    }

    @VisibleForTesting
    static void validatePartitionStatistics(SchemaTableName table, Map<String, PartitionStatistics> partitionStatistics)
    {
        partitionStatistics.forEach((partition, statistics) -> {
            HiveBasicStatistics basicStatistics = statistics.getBasicStatistics();
            OptionalLong rowCount = basicStatistics.getRowCount();
            rowCount.ifPresent(count -> checkStatistics(count >= 0, table, partition, "rowCount must be greater than or equal to zero: %s", count));
            basicStatistics.getFileCount().ifPresent(count -> checkStatistics(count >= 0, table, partition, "fileCount must be greater than or equal to zero: %s", count));
            basicStatistics.getInMemoryDataSizeInBytes().ifPresent(size -> checkStatistics(size >= 0, table, partition, "inMemoryDataSizeInBytes must be greater than or equal to zero: %s", size));
            basicStatistics.getOnDiskDataSizeInBytes().ifPresent(size -> checkStatistics(size >= 0, table, partition, "onDiskDataSizeInBytes must be greater than or equal to zero: %s", size));
            statistics.getColumnStatistics().forEach((column, columnStatistics) -> validateColumnStatistics(table, partition, column, rowCount, columnStatistics));
        });
    }

    private static void validateColumnStatistics(SchemaTableName table, String partition, String column, OptionalLong rowCount, HiveColumnStatistics columnStatistics)
    {
        columnStatistics.getMaxValueSizeInBytes().ifPresent(maxValueSizeInBytes ->
                checkStatistics(maxValueSizeInBytes >= 0, table, partition, column, "maxValueSizeInBytes must be greater than or equal to zero: %s", maxValueSizeInBytes));
        columnStatistics.getTotalSizeInBytes().ifPresent(totalSizeInBytes ->
                checkStatistics(totalSizeInBytes >= 0, table, partition, column, "totalSizeInBytes must be greater than or equal to zero: %s", totalSizeInBytes));
        columnStatistics.getNullsCount().ifPresent(nullsCount -> {
            checkStatistics(nullsCount >= 0, table, partition, column, "nullsCount must be greater than or equal to zero: %s", nullsCount);
            if (rowCount.isPresent()) {
                checkStatistics(
                        nullsCount <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "nullsCount must be less than or equal to rowCount. nullsCount: %s. rowCount: %s.",
                        nullsCount,
                        rowCount.getAsLong());
            }
        });
        columnStatistics.getDistinctValuesCount().ifPresent(distinctValuesCount -> {
            checkStatistics(distinctValuesCount >= 0, table, partition, column, "distinctValuesCount must be greater than or equal to zero: %s", distinctValuesCount);
            if (rowCount.isPresent()) {
                checkStatistics(
                        distinctValuesCount <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "distinctValuesCount must be less than or equal to rowCount. distinctValuesCount: %s. rowCount: %s.",
                        distinctValuesCount,
                        rowCount.getAsLong());
            }
            if (rowCount.isPresent() && columnStatistics.getNullsCount().isPresent()) {
                long nonNullsCount = rowCount.getAsLong() - columnStatistics.getNullsCount().getAsLong();
                checkStatistics(
                        distinctValuesCount <= nonNullsCount,
                        table,
                        partition,
                        column,
                        "distinctValuesCount must be less than or equal to nonNullsCount. distinctValuesCount: %s. nonNullsCount: %s.",
                        distinctValuesCount,
                        nonNullsCount);
            }
        });

        columnStatistics.getIntegerStatistics().ifPresent(integerStatistics -> {
            OptionalLong min = integerStatistics.getMin();
            OptionalLong max = integerStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.getAsLong() <= max.getAsLong(),
                        table,
                        partition,
                        column,
                        "integerStatistics.min must be less than or equal to integerStatistics.max. integerStatistics.min: %s. integerStatistics.max: %s.",
                        min.getAsLong(),
                        max.getAsLong());
            }
        });
        columnStatistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
            OptionalDouble min = doubleStatistics.getMin();
            OptionalDouble max = doubleStatistics.getMax();
            if (min.isPresent() && max.isPresent() && !isNaN(min.getAsDouble()) && !isNaN(max.getAsDouble())) {
                checkStatistics(
                        min.getAsDouble() <= max.getAsDouble(),
                        table,
                        partition,
                        column,
                        "doubleStatistics.min must be less than or equal to doubleStatistics.max. doubleStatistics.min: %s. doubleStatistics.max: %s.",
                        min.getAsDouble(),
                        max.getAsDouble());
            }
        });
        columnStatistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
            Optional<BigDecimal> min = decimalStatistics.getMin();
            Optional<BigDecimal> max = decimalStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.get().compareTo(max.get()) <= 0,
                        table,
                        partition,
                        column,
                        "decimalStatistics.min must be less than or equal to decimalStatistics.max. decimalStatistics.min: %s. decimalStatistics.max: %s.",
                        min.get(),
                        max.get());
            }
        });
        columnStatistics.getDateStatistics().ifPresent(dateStatistics -> {
            Optional<LocalDate> min = dateStatistics.getMin();
            Optional<LocalDate> max = dateStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.get().compareTo(max.get()) <= 0,
                        table,
                        partition,
                        column,
                        "dateStatistics.min must be less than or equal to dateStatistics.max. dateStatistics.min: %s. dateStatistics.max: %s.",
                        min.get(),
                        max.get());
            }
        });
        columnStatistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
            OptionalLong falseCount = booleanStatistics.getFalseCount();
            OptionalLong trueCount = booleanStatistics.getTrueCount();
            falseCount.ifPresent(count ->
                    checkStatistics(count >= 0, table, partition, column, "falseCount must be greater than or equal to zero: %s", count));
            trueCount.ifPresent(count ->
                    checkStatistics(count >= 0, table, partition, column, "trueCount must be greater than or equal to zero: %s", count));
            if (rowCount.isPresent() && falseCount.isPresent()) {
                checkStatistics(
                        falseCount.getAsLong() <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "booleanStatistics.falseCount must be less than or equal to rowCount. booleanStatistics.falseCount: %s. rowCount: %s.",
                        falseCount.getAsLong(),
                        rowCount.getAsLong());
            }
            if (rowCount.isPresent() && trueCount.isPresent()) {
                checkStatistics(
                        trueCount.getAsLong() <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "booleanStatistics.trueCount must be less than or equal to rowCount. booleanStatistics.trueCount: %s. rowCount: %s.",
                        trueCount.getAsLong(),
                        rowCount.getAsLong());
            }
        });
    }

    private static void checkStatistics(boolean expression, SchemaTableName table, String partition, String column, String message, Object... args)
    {
        if (!expression) {
            throw new PrestoException(
                    HIVE_CORRUPTED_COLUMN_STATISTICS,
                    format("Corrupted partition statistics (Table: %s Partition: [%s] Column: %s): %s", table, partition, column, format(message, args)));
        }
    }

    private static void checkStatistics(boolean expression, SchemaTableName table, String partition, String message, Object... args)
    {
        if (!expression) {
            throw new PrestoException(
                    HIVE_CORRUPTED_COLUMN_STATISTICS,
                    format("Corrupted partition statistics (Table: %s Partition: [%s]): %s", table, partition, format(message, args)));
        }
    }

    private static TableStatistics getTableStatistics(
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics)
    {
        if (statistics.isEmpty()) {
            return TableStatistics.empty();
        }

        checkArgument(!partitions.isEmpty(), "partitions is empty");

        OptionalDouble optionalAverageRowsPerPartition = calculateAverageRowsPerPartition(statistics.values());
        if (!optionalAverageRowsPerPartition.isPresent()) {
            return TableStatistics.empty();
        }
        double averageRowsPerPartition = optionalAverageRowsPerPartition.getAsDouble();
        verify(averageRowsPerPartition >= 0, "averageRowsPerPartition must be greater than or equal to zero");
        int queriedPartitionsCount = partitions.size();
        double rowCount = averageRowsPerPartition * queriedPartitionsCount;

        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(rowCount));

        OptionalDouble optionalAverageSizePerPartition = calculateAverageSizePerPartition(statistics.values());
        if (optionalAverageSizePerPartition.isPresent()) {
            double averageSizePerPartition = optionalAverageSizePerPartition.getAsDouble();
            verify(averageSizePerPartition >= 0, "averageSizePerPartition must be greater than or equal to zero: %s", averageSizePerPartition);
            double totalSize = averageSizePerPartition * queriedPartitionsCount;
            result.setTotalSize(Estimate.of(totalSize));
        }

        for (Map.Entry<String, ColumnHandle> column : columns.entrySet()) {
            String columnName = column.getKey();
            HiveColumnHandle columnHandle = (HiveColumnHandle) column.getValue();
            Type columnType = columnTypes.get(columnName);
            ColumnStatistics columnStatistics;
            if (columnHandle.isPartitionKey()) {
                columnStatistics = createPartitionColumnStatistics(columnHandle, columnType, partitions, statistics, averageRowsPerPartition, rowCount);
            }
            else {
                columnStatistics = createDataColumnStatistics(columnName, columnType, rowCount, statistics.values());
            }
            result.setColumnStatistics(columnHandle, columnStatistics);
        }
        return result.build();
    }

    @VisibleForTesting
    static OptionalDouble calculateAverageRowsPerPartition(Collection<PartitionStatistics> statistics)
    {
        return statistics.stream()
                .map(PartitionStatistics::getBasicStatistics)
                .map(HiveBasicStatistics::getRowCount)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .peek(count -> verify(count >= 0, "count must be greater than or equal to zero"))
                .average();
    }

    @VisibleForTesting
    static OptionalDouble calculateAverageSizePerPartition(Collection<PartitionStatistics> statistics)
    {
        return statistics.stream()
                .map(PartitionStatistics::getBasicStatistics)
                .map(HiveBasicStatistics::getInMemoryDataSizeInBytes)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .peek(size -> verify(size >= 0, "size must be greater than or equal to zero"))
                .average();
    }

    private static ColumnStatistics createPartitionColumnStatistics(
            HiveColumnHandle column,
            Type type,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition,
            double rowCount)
    {
        List<HivePartition> nonEmptyPartitions = partitions.stream()
                .filter(partition -> getPartitionRowCount(partition.getPartitionId(), statistics).orElse(averageRowsPerPartition) != 0)
                .collect(toImmutableList());

        return ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(calculateDistinctPartitionKeys(column, nonEmptyPartitions)))
                .setNullsFraction(Estimate.of(calculateNullsFractionForPartitioningKey(column, partitions, statistics, averageRowsPerPartition, rowCount)))
                .setRange(calculateRangeForPartitioningKey(column, type, nonEmptyPartitions))
                .setDataSize(calculateDataSizeForPartitioningKey(column, type, partitions, statistics, averageRowsPerPartition))
                .build();
    }

    @VisibleForTesting
    static long calculateDistinctPartitionKeys(
            HiveColumnHandle column,
            List<HivePartition> partitions)
    {
        return partitions.stream()
                .map(partition -> partition.getKeys().get(column))
                .filter(value -> !value.isNull())
                .distinct()
                .count();
    }

    @VisibleForTesting
    static double calculateNullsFractionForPartitioningKey(
            HiveColumnHandle column,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition,
            double rowCount)
    {
        if (rowCount == 0) {
            return 0;
        }
        double estimatedNullsCount = partitions.stream()
                .filter(partition -> partition.getKeys().get(column).isNull())
                .map(HivePartition::getPartitionId)
                .mapToDouble(partitionName -> getPartitionRowCount(partitionName, statistics).orElse(averageRowsPerPartition))
                .sum();
        return normalizeFraction(estimatedNullsCount / rowCount);
    }

    private static double normalizeFraction(double fraction)
    {
        checkArgument(!isNaN(fraction), "fraction is NaN");
        checkArgument(isFinite(fraction), "fraction must be finite");
        if (fraction < 0) {
            return 0;
        }
        if (fraction > 1) {
            return 1;
        }
        return fraction;
    }

    @VisibleForTesting
    static Estimate calculateDataSizeForPartitioningKey(
            HiveColumnHandle column,
            Type type,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition)
    {
        if (!hasDataSize(type)) {
            return Estimate.unknown();
        }
        double dataSize = 0;
        for (HivePartition partition : partitions) {
            int length = getSize(partition.getKeys().get(column));
            double rowCount = getPartitionRowCount(partition.getPartitionId(), statistics).orElse(averageRowsPerPartition);
            dataSize += length * rowCount;
        }
        return Estimate.of(dataSize);
    }

    private static boolean hasDataSize(Type type)
    {
        return isVarcharType(type) || isCharType(type);
    }

    private static int getSize(NullableValue nullableValue)
    {
        if (nullableValue.isNull()) {
            return 0;
        }
        Object value = nullableValue.getValue();
        checkArgument(value instanceof Slice, "value is expected to be of Slice type");
        return ((Slice) value).length();
    }

    private static OptionalDouble getPartitionRowCount(String partitionName, Map<String, PartitionStatistics> statistics)
    {
        PartitionStatistics partitionStatistics = statistics.get(partitionName);
        if (partitionStatistics == null) {
            return OptionalDouble.empty();
        }
        OptionalLong rowCount = partitionStatistics.getBasicStatistics().getRowCount();
        if (rowCount.isPresent()) {
            verify(rowCount.getAsLong() >= 0, "rowCount must be greater than or equal to zero");
            return OptionalDouble.of(rowCount.getAsLong());
        }
        return OptionalDouble.empty();
    }

    @VisibleForTesting
    static Optional<DoubleRange> calculateRangeForPartitioningKey(HiveColumnHandle column, Type type, List<HivePartition> partitions)
    {
        if (!isRangeSupported(type)) {
            return Optional.empty();
        }

        List<Double> values = partitions.stream()
                .map(HivePartition::getKeys)
                .map(keys -> keys.get(column))
                .filter(value -> !value.isNull())
                .map(NullableValue::getValue)
                .map(value -> convertPartitionValueToDouble(type, value))
                .collect(toImmutableList());

        if (values.isEmpty()) {
            return Optional.empty();
        }

        double min = values.get(0);
        double max = values.get(0);

        for (Double value : values) {
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        return Optional.of(new DoubleRange(min, max));
    }

    @VisibleForTesting
    static double convertPartitionValueToDouble(Type type, Object value)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return (Long) value;
        }
        if (type.equals(DOUBLE)) {
            return (Double) value;
        }
        if (type.equals(REAL)) {
            return intBitsToFloat(((Long) value).intValue());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (isShortDecimal(decimalType)) {
                return parseDouble(Decimals.toString((Long) value, decimalType.getScale()));
            }
            if (isLongDecimal(decimalType)) {
                return parseDouble(Decimals.toString((Slice) value, decimalType.getScale()));
            }
            throw new IllegalArgumentException("Unexpected decimal type: " + decimalType);
        }
        if (type.equals(DATE)) {
            return (Long) value;
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    @VisibleForTesting
    static ColumnStatistics createDataColumnStatistics(String column, Type type, double rowsCount, Collection<PartitionStatistics> partitionStatistics)
    {
        List<HiveColumnStatistics> columnStatistics = partitionStatistics.stream()
                .map(PartitionStatistics::getColumnStatistics)
                .map(statistics -> statistics.get(column))
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (columnStatistics.isEmpty()) {
            return ColumnStatistics.empty();
        }

        return ColumnStatistics.builder()
                .setDistinctValuesCount(calculateDistinctValuesCount(columnStatistics))
                .setNullsFraction(calculateNullsFraction(column, partitionStatistics))
                .setDataSize(calculateDataSize(column, partitionStatistics, rowsCount))
                .setRange(calculateRange(type, columnStatistics))
                .build();
    }

    @VisibleForTesting
    static Estimate calculateDistinctValuesCount(List<HiveColumnStatistics> columnStatistics)
    {
        return columnStatistics.stream()
                .map(MetastoreHiveStatisticsProvider::getDistinctValuesCount)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .peek(distinctValuesCount -> verify(distinctValuesCount >= 0, "distinctValuesCount must be greater than or equal to zero"))
                .max(Long::compare)
                .map(Estimate::of)
                .orElse(Estimate.unknown());
    }

    private static OptionalLong getDistinctValuesCount(HiveColumnStatistics statistics)
    {
        if (statistics.getBooleanStatistics().isPresent() &&
                statistics.getBooleanStatistics().get().getFalseCount().isPresent() &&
                statistics.getBooleanStatistics().get().getTrueCount().isPresent()) {
            long falseCount = statistics.getBooleanStatistics().get().getFalseCount().getAsLong();
            long trueCount = statistics.getBooleanStatistics().get().getTrueCount().getAsLong();
            return OptionalLong.of((falseCount > 0 ? 1 : 0) + (trueCount > 0 ? 1 : 0));
        }
        if (statistics.getDistinctValuesCount().isPresent()) {
            return statistics.getDistinctValuesCount();
        }
        return OptionalLong.empty();
    }

    @VisibleForTesting
    static Estimate calculateNullsFraction(String column, Collection<PartitionStatistics> partitionStatistics)
    {
        List<PartitionStatistics> statisticsWithKnownRowCountAndNullsCount = partitionStatistics.stream()
                .filter(statistics -> {
                    if (!statistics.getBasicStatistics().getRowCount().isPresent()) {
                        return false;
                    }
                    HiveColumnStatistics columnStatistics = statistics.getColumnStatistics().get(column);
                    if (columnStatistics == null) {
                        return false;
                    }
                    return columnStatistics.getNullsCount().isPresent();
                })
                .collect(toImmutableList());

        if (statisticsWithKnownRowCountAndNullsCount.isEmpty()) {
            return Estimate.unknown();
        }

        long totalNullsCount = 0;
        long totalRowCount = 0;
        for (PartitionStatistics statistics : statisticsWithKnownRowCountAndNullsCount) {
            long rowCount = statistics.getBasicStatistics().getRowCount().orElseThrow(() -> new VerifyException("rowCount is not present"));
            verify(rowCount >= 0, "rowCount must be greater than or equal to zero");
            HiveColumnStatistics columnStatistics = statistics.getColumnStatistics().get(column);
            verify(columnStatistics != null, "columnStatistics is null");
            long nullsCount = columnStatistics.getNullsCount().orElseThrow(() -> new VerifyException("nullsCount is not present"));
            verify(nullsCount >= 0, "nullsCount must be greater than or equal to zero");
            verify(nullsCount <= rowCount, "nullsCount must be less than or equal to rowCount. nullsCount: %s. rowCount: %s.", nullsCount, rowCount);
            totalNullsCount += nullsCount;
            totalRowCount += rowCount;
        }

        if (totalRowCount == 0) {
            return Estimate.zero();
        }

        verify(
                totalNullsCount <= totalRowCount,
                "totalNullsCount must be less than or equal to totalRowCount. totalNullsCount: %s. totalRowCount: %s.",
                totalNullsCount,
                totalRowCount);
        return Estimate.of(((double) totalNullsCount) / totalRowCount);
    }

    @VisibleForTesting
    static Estimate calculateDataSize(String column, Collection<PartitionStatistics> partitionStatistics, double totalRowCount)
    {
        List<PartitionStatistics> statisticsWithKnownRowCountAndDataSize = partitionStatistics.stream()
                .filter(statistics -> {
                    if (!statistics.getBasicStatistics().getRowCount().isPresent()) {
                        return false;
                    }
                    HiveColumnStatistics columnStatistics = statistics.getColumnStatistics().get(column);
                    if (columnStatistics == null) {
                        return false;
                    }
                    return columnStatistics.getTotalSizeInBytes().isPresent();
                })
                .collect(toImmutableList());

        if (statisticsWithKnownRowCountAndDataSize.isEmpty()) {
            return Estimate.unknown();
        }

        long knownRowCount = 0;
        long knownDataSize = 0;
        for (PartitionStatistics statistics : statisticsWithKnownRowCountAndDataSize) {
            long rowCount = statistics.getBasicStatistics().getRowCount().orElseThrow(() -> new VerifyException("rowCount is not present"));
            verify(rowCount >= 0, "rowCount must be greater than or equal to zero");
            HiveColumnStatistics columnStatistics = statistics.getColumnStatistics().get(column);
            verify(columnStatistics != null, "columnStatistics is null");
            long dataSize = columnStatistics.getTotalSizeInBytes().orElseThrow(() -> new VerifyException("totalSizeInBytes is not present"));
            verify(dataSize >= 0, "dataSize must be greater than or equal to zero");
            knownRowCount += rowCount;
            knownDataSize += dataSize;
        }

        if (totalRowCount == 0) {
            return Estimate.zero();
        }

        if (knownRowCount == 0) {
            return Estimate.unknown();
        }

        double averageValueDataSizeInBytes = ((double) knownDataSize) / knownRowCount;
        return Estimate.of(averageValueDataSizeInBytes * totalRowCount);
    }

    @VisibleForTesting
    static Optional<DoubleRange> calculateRange(Type type, List<HiveColumnStatistics> columnStatistics)
    {
        if (!isRangeSupported(type)) {
            return Optional.empty();
        }
        return columnStatistics.stream()
                .map(statistics -> createRange(type, statistics))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(DoubleRange::union);
    }

    private static boolean isRangeSupported(Type type)
    {
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type.equals(DATE)
                || type instanceof DecimalType;
    }

    private static Optional<DoubleRange> createRange(Type type, HiveColumnStatistics statistics)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return statistics.getIntegerStatistics().flatMap(integerStatistics -> createIntegerRange(type, integerStatistics));
        }
        if (type.equals(DOUBLE) || type.equals(REAL)) {
            return statistics.getDoubleStatistics().flatMap(MetastoreHiveStatisticsProvider::createDoubleRange);
        }
        if (type.equals(DATE)) {
            return statistics.getDateStatistics().flatMap(MetastoreHiveStatisticsProvider::createDateRange);
        }
        if (type instanceof DecimalType) {
            return statistics.getDecimalStatistics().flatMap(MetastoreHiveStatisticsProvider::createDecimalRange);
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    private static Optional<DoubleRange> createIntegerRange(Type type, IntegerStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(createIntegerRange(type, statistics.getMin().getAsLong(), statistics.getMax().getAsLong()));
        }
        return Optional.empty();
    }

    private static DoubleRange createIntegerRange(Type type, long min, long max)
    {
        return new DoubleRange(normalizeIntegerValue(type, min), normalizeIntegerValue(type, max));
    }

    private static long normalizeIntegerValue(Type type, long value)
    {
        if (type.equals(BIGINT)) {
            return value;
        }
        if (type.equals(INTEGER)) {
            return Ints.saturatedCast(value);
        }
        if (type.equals(SMALLINT)) {
            return Shorts.saturatedCast(value);
        }
        if (type.equals(TINYINT)) {
            return SignedBytes.saturatedCast(value);
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    private static Optional<DoubleRange> createDoubleRange(DoubleStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent() && !isNaN(statistics.getMin().getAsDouble()) && !isNaN(statistics.getMax().getAsDouble())) {
            return Optional.of(new DoubleRange(statistics.getMin().getAsDouble(), statistics.getMax().getAsDouble()));
        }
        return Optional.empty();
    }

    private static Optional<DoubleRange> createDateRange(DateStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(new DoubleRange(statistics.getMin().get().toEpochDay(), statistics.getMax().get().toEpochDay()));
        }
        return Optional.empty();
    }

    private static Optional<DoubleRange> createDecimalRange(DecimalStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(new DoubleRange(statistics.getMin().get().doubleValue(), statistics.getMax().get().doubleValue()));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    interface PartitionsStatisticsProvider
    {
        Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions);
    }
}
