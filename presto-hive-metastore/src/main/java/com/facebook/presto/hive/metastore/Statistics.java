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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createPartitionValues;
import static com.facebook.presto.hive.metastore.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.metastore.Statistics.ReduceOperator.MAX;
import static com.facebook.presto.hive.metastore.Statistics.ReduceOperator.MIN;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class Statistics
{
    private Statistics() {}

    public static PartitionStatistics merge(PartitionStatistics first, PartitionStatistics second)
    {
        return new PartitionStatistics(
                reduce(first.getBasicStatistics(), second.getBasicStatistics(), ADD),
                merge(first.getColumnStatistics(), second.getColumnStatistics()));
    }

    public static HiveBasicStatistics reduce(HiveBasicStatistics first, HiveBasicStatistics second, ReduceOperator operator)
    {
        return new HiveBasicStatistics(
                reduce(first.getFileCount(), second.getFileCount(), operator, false),
                reduce(first.getRowCount(), second.getRowCount(), operator, false),
                reduce(first.getInMemoryDataSizeInBytes(), second.getInMemoryDataSizeInBytes(), operator, false),
                reduce(first.getOnDiskDataSizeInBytes(), second.getOnDiskDataSizeInBytes(), operator, false));
    }

    public static Map<String, HiveColumnStatistics> merge(Map<String, HiveColumnStatistics> first, Map<String, HiveColumnStatistics> second)
    {
        // only keep columns that have statistics for both sides
        Set<String> columns = intersection(first.keySet(), second.keySet());
        return columns.stream()
                .collect(toImmutableMap(
                        column -> column,
                        column -> merge(first.get(column), second.get(column))));
    }

    public static HiveColumnStatistics merge(HiveColumnStatistics first, HiveColumnStatistics second)
    {
        return new HiveColumnStatistics(
                mergeIntegerStatistics(first.getIntegerStatistics(), second.getIntegerStatistics()),
                mergeDoubleStatistics(first.getDoubleStatistics(), second.getDoubleStatistics()),
                mergeDecimalStatistics(first.getDecimalStatistics(), second.getDecimalStatistics()),
                mergeDateStatistics(first.getDateStatistics(), second.getDateStatistics()),
                mergeBooleanStatistics(first.getBooleanStatistics(), second.getBooleanStatistics()),
                reduce(first.getMaxValueSizeInBytes(), second.getMaxValueSizeInBytes(), MAX, true),
                reduce(first.getTotalSizeInBytes(), second.getTotalSizeInBytes(), ADD, true),
                reduce(first.getNullsCount(), second.getNullsCount(), ADD, false),
                reduce(first.getDistinctValuesCount(), second.getDistinctValuesCount(), MAX, false));
    }

    private static Optional<IntegerStatistics> mergeIntegerStatistics(Optional<IntegerStatistics> first, Optional<IntegerStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new IntegerStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<DoubleStatistics> mergeDoubleStatistics(Optional<DoubleStatistics> first, Optional<DoubleStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DoubleStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<DecimalStatistics> mergeDecimalStatistics(Optional<DecimalStatistics> first, Optional<DecimalStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DecimalStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<DateStatistics> mergeDateStatistics(Optional<DateStatistics> first, Optional<DateStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DateStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<BooleanStatistics> mergeBooleanStatistics(Optional<BooleanStatistics> first, Optional<BooleanStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new BooleanStatistics(
                    reduce(first.get().getTrueCount(), second.get().getTrueCount(), ADD, false),
                    reduce(first.get().getFalseCount(), second.get().getFalseCount(), ADD, false)));
        }
        return Optional.empty();
    }

    private static OptionalLong reduce(OptionalLong first, OptionalLong second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT:
                    return OptionalLong.of(first.getAsLong() - second.getAsLong());
                case MAX:
                    return OptionalLong.of(max(first.getAsLong(), second.getAsLong()));
                case MIN:
                    return OptionalLong.of(min(first.getAsLong(), second.getAsLong()));
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalLong.empty();
    }

    private static OptionalDouble reduce(OptionalDouble first, OptionalDouble second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalDouble.of(first.getAsDouble() + second.getAsDouble());
                case SUBTRACT:
                    return OptionalDouble.of(first.getAsDouble() - second.getAsDouble());
                case MAX:
                    return OptionalDouble.of(max(first.getAsDouble(), second.getAsDouble()));
                case MIN:
                    return OptionalDouble.of(min(first.getAsDouble(), second.getAsDouble()));
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalDouble.empty();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<? super T>> Optional<T> reduce(Optional<T> first, Optional<T> second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case MAX:
                    return Optional.of(max(first.get(), second.get()));
                case MIN:
                    return Optional.of(min(first.get(), second.get()));
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return Optional.empty();
    }

    private static <T extends Comparable<? super T>> T max(T first, T second)
    {
        return first.compareTo(second) >= 0 ? first : second;
    }

    private static <T extends Comparable<? super T>> T min(T first, T second)
    {
        return first.compareTo(second) <= 0 ? first : second;
    }

    public static PartitionStatistics createEmptyPartitionStatistics(Map<String, Type> columnTypes, Map<String, Set<ColumnStatisticType>> columnStatisticsMetadataTypes)
    {
        Map<String, HiveColumnStatistics> columnStatistics = columnStatisticsMetadataTypes.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> createColumnStatisticsForEmptyPartition(columnTypes.get(entry.getKey()), entry.getValue())));
        return new PartitionStatistics(createZeroStatistics(), columnStatistics);
    }

    private static HiveColumnStatistics createColumnStatisticsForEmptyPartition(Type columnType, Set<ColumnStatisticType> columnStatisticTypes)
    {
        requireNonNull(columnType, "columnType is null");
        HiveColumnStatistics.Builder result = HiveColumnStatistics.builder();
        for (ColumnStatisticType columnStatisticType : columnStatisticTypes) {
            switch (columnStatisticType) {
                case MAX_VALUE_SIZE_IN_BYTES:
                    result.setMaxValueSizeInBytes(0);
                    break;
                case TOTAL_SIZE_IN_BYTES:
                    result.setTotalSizeInBytes(0);
                    break;
                case NUMBER_OF_DISTINCT_VALUES:
                    result.setDistinctValuesCount(0);
                    break;
                case NUMBER_OF_NON_NULL_VALUES:
                    result.setNullsCount(0);
                    break;
                case NUMBER_OF_TRUE_VALUES:
                    result.setBooleanStatistics(new BooleanStatistics(OptionalLong.of(0L), OptionalLong.of(0L)));
                    break;
                case MIN_VALUE:
                case MAX_VALUE:
                    setMinMaxForEmptyPartition(columnType, result);
                    break;
                default:
                    throw new PrestoException(HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE, "Unknown column statistics type: " + columnStatisticType.name());
            }
        }
        return result.build();
    }

    private static void setMinMaxForEmptyPartition(Type type, HiveColumnStatistics.Builder result)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            result.setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()));
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            result.setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));
        }
        else if (type.equals(DATE)) {
            result.setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty()));
        }
        else if (type.equals(TIMESTAMP)) {
            result.setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()));
        }
        else if (type instanceof DecimalType) {
            result.setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()));
        }
        else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    public static Map<List<String>, ComputedStatistics> createComputedStatisticsToPartitionMap(
            Collection<ComputedStatistics> computedStatistics,
            List<String> partitionColumns,
            Map<String, Type> columnTypes)
    {
        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(columnTypes::get)
                .collect(toImmutableList());

        return computedStatistics.stream()
                .collect(toImmutableMap(statistics -> getPartitionValues(statistics, partitionColumns, partitionColumnTypes), statistics -> statistics));
    }

    private static List<String> getPartitionValues(ComputedStatistics statistics, List<String> partitionColumns, List<Type> partitionColumnTypes)
    {
        checkArgument(statistics.getGroupingColumns().equals(partitionColumns),
                "Unexpected grouping. Partition columns: %s. Grouping columns: %s", partitionColumns, statistics.getGroupingColumns());
        Page partitionColumnsPage = new Page(1, statistics.getGroupingValues().toArray(new Block[] {}));
        return createPartitionValues(partitionColumnTypes, partitionColumnsPage, 0);
    }

    public static Map<String, HiveColumnStatistics> fromComputedStatistics(
            ConnectorSession session,
            DateTimeZone timeZone,
            Map<ColumnStatisticMetadata, Block> computedStatistics,
            Map<String, Type> columnTypes,
            long rowCount)
    {
        return createColumnToComputedStatisticsMap(computedStatistics).entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> createHiveColumnStatistics(session, timeZone, entry.getValue(), columnTypes.get(entry.getKey()), rowCount)));
    }

    private static Map<String, Map<ColumnStatisticType, Block>> createColumnToComputedStatisticsMap(Map<ColumnStatisticMetadata, Block> computedStatistics)
    {
        Map<String, Map<ColumnStatisticType, Block>> result = new HashMap<>();
        computedStatistics.forEach((metadata, block) -> {
            Map<ColumnStatisticType, Block> columnStatistics = result.computeIfAbsent(metadata.getColumnName(), key -> new HashMap<>());
            columnStatistics.put(metadata.getStatisticType(), block);
        });
        return result.entrySet()
                .stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
    }

    private static HiveColumnStatistics createHiveColumnStatistics(
            ConnectorSession session,
            DateTimeZone timeZone,
            Map<ColumnStatisticType, Block> computedStatistics,
            Type columnType,
            long rowCount)
    {
        HiveColumnStatistics.Builder result = HiveColumnStatistics.builder();

        // MIN_VALUE, MAX_VALUE
        // We ask the engine to compute either both or neither
        verify(computedStatistics.containsKey(MIN_VALUE) == computedStatistics.containsKey(MAX_VALUE));
        if (computedStatistics.containsKey(MIN_VALUE)) {
            setMinMax(session, timeZone, columnType, computedStatistics.get(MIN_VALUE), computedStatistics.get(MAX_VALUE), result);
        }

        // MAX_VALUE_SIZE_IN_BYTES
        if (computedStatistics.containsKey(MAX_VALUE_SIZE_IN_BYTES)) {
            result.setMaxValueSizeInBytes(getIntegerValue(session, BIGINT, computedStatistics.get(MAX_VALUE_SIZE_IN_BYTES)));
        }

        // TOTAL_VALUES_SIZE_IN_BYTES
        if (computedStatistics.containsKey(TOTAL_SIZE_IN_BYTES)) {
            result.setTotalSizeInBytes(getIntegerValue(session, BIGINT, computedStatistics.get(TOTAL_SIZE_IN_BYTES)));
        }

        // NUMBER OF NULLS
        if (computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            result.setNullsCount(rowCount - BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0));
        }

        // NDV
        if (computedStatistics.containsKey(NUMBER_OF_DISTINCT_VALUES) && computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            // number of distinct value is estimated using HLL, and can be higher than the number of non null values
            long numberOfNonNullValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0);
            long numberOfDistinctValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_DISTINCT_VALUES), 0);
            if (numberOfDistinctValues > numberOfNonNullValues) {
                result.setDistinctValuesCount(numberOfNonNullValues);
            }
            else {
                result.setDistinctValuesCount(numberOfDistinctValues);
            }
        }

        // NUMBER OF FALSE, NUMBER OF TRUE
        if (computedStatistics.containsKey(NUMBER_OF_TRUE_VALUES) && computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            long numberOfTrue = BIGINT.getLong(computedStatistics.get(NUMBER_OF_TRUE_VALUES), 0);
            long numberOfNonNullValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0);
            result.setBooleanStatistics(new BooleanStatistics(OptionalLong.of(numberOfTrue), OptionalLong.of(numberOfNonNullValues - numberOfTrue)));
        }
        return result.build();
    }

    private static void setMinMax(ConnectorSession session, DateTimeZone timeZone, Type type, Block min, Block max, HiveColumnStatistics.Builder result)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            result.setIntegerStatistics(new IntegerStatistics(getIntegerValue(session, type, min), getIntegerValue(session, type, max)));
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            result.setDoubleStatistics(new DoubleStatistics(getDoubleValue(session, type, min), getDoubleValue(session, type, max)));
        }
        else if (type.equals(DATE)) {
            result.setDateStatistics(new DateStatistics(getDateValue(session, type, min), getDateValue(session, type, max)));
        }
        else if (type.equals(TIMESTAMP)) {
            result.setIntegerStatistics(new IntegerStatistics(getTimestampValue(timeZone, min), getTimestampValue(timeZone, max)));
        }
        else if (type instanceof DecimalType) {
            result.setDecimalStatistics(new DecimalStatistics(getDecimalValue(session, type, min), getDecimalValue(session, type, max)));
        }
        else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static OptionalLong getIntegerValue(ConnectorSession session, Type type, Block block)
    {
        // works for BIGINT as well as for other integer types TINYINT/SMALLINT/INTEGER that store values as byte/short/int
        return block.isNull(0) ? OptionalLong.empty() : OptionalLong.of(((Number) type.getObjectValue(session.getSqlFunctionProperties(), block, 0)).longValue());
    }

    private static OptionalDouble getDoubleValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? OptionalDouble.empty() : OptionalDouble.of(((Number) type.getObjectValue(session.getSqlFunctionProperties(), block, 0)).doubleValue());
    }

    private static Optional<LocalDate> getDateValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? Optional.empty() : Optional.of(LocalDate.ofEpochDay(((SqlDate) type.getObjectValue(session.getSqlFunctionProperties(), block, 0)).getDays()));
    }

    private static OptionalLong getTimestampValue(DateTimeZone timeZone, Block block)
    {
        // TODO #7122
        return block.isNull(0) ? OptionalLong.empty() : OptionalLong.of(MILLISECONDS.toSeconds(timeZone.convertUTCToLocal(block.getLong(0))));
    }

    private static Optional<BigDecimal> getDecimalValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? Optional.empty() : Optional.of(((SqlDecimal) type.getObjectValue(session.getSqlFunctionProperties(), block, 0)).toBigDecimal());
    }

    public enum ReduceOperator
    {
        ADD,
        SUBTRACT,
        MIN,
        MAX,
    }
}
