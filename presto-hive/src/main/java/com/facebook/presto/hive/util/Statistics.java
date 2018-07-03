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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.hive.HiveWriteUtils.createPartitionValues;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.SELECT_MAX;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.SELECT_MIN;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.AVERAGE_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.intersection;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public final class Statistics
{
    private Statistics() {}

    public static PartitionStatistics merge(PartitionStatistics first, PartitionStatistics second)
    {
        return new PartitionStatistics(
                reduce(first.getBasicStatistics(), second.getBasicStatistics(), ADD),
                merge(first.getColumnStatistics(), first.getBasicStatistics().getRowCount(), second.getColumnStatistics(), second.getBasicStatistics().getRowCount()));
    }

    public static HiveBasicStatistics reduce(HiveBasicStatistics first, HiveBasicStatistics second, ReduceOperator operator)
    {
        return new HiveBasicStatistics(
                reduce(first.getFileCount(), second.getFileCount(), operator),
                reduce(first.getRowCount(), second.getRowCount(), operator),
                reduce(first.getInMemoryDataSizeInBytes(), second.getInMemoryDataSizeInBytes(), operator),
                reduce(first.getOnDiskDataSizeInBytes(), second.getOnDiskDataSizeInBytes(), operator));
    }

    public static Map<String, HiveColumnStatistics> merge(
            Map<String, HiveColumnStatistics> first, OptionalLong firstRowCount, Map<String, HiveColumnStatistics> second, OptionalLong secondRowCount)
    {
        // skip the columns for which statistics from either of sides are missing
        Set<String> columns = intersection(first.keySet(), second.keySet());
        return columns.stream()
                .collect(toImmutableMap(column -> column, column -> merge(first.get(column), firstRowCount, second.get(column), secondRowCount)));
    }

    public static HiveColumnStatistics merge(HiveColumnStatistics first, OptionalLong firstRowCount, HiveColumnStatistics second, OptionalLong secondRowCount)
    {
        return new HiveColumnStatistics(
                first.getLowValue().isPresent() ? reduce(first.getLowValue(), second.getLowValue(), SELECT_MIN) : second.getLowValue(),
                first.getHighValue().isPresent() ? reduce(first.getHighValue(), second.getHighValue(), SELECT_MAX) : second.getHighValue(),
                reduce(first.getMaxColumnLength(), second.getMaxColumnLength(), SELECT_MAX),
                mergeAvg(first.getAverageColumnLength(), firstRowCount, second.getAverageColumnLength(), secondRowCount),
                reduce(first.getTrueCount(), second.getTrueCount(), ADD),
                reduce(first.getFalseCount(), second.getFalseCount(), ADD),
                reduce(first.getNullsCount(), second.getNullsCount(), ADD),
                reduce(first.getDistinctValuesCount(), second.getDistinctValuesCount(), SELECT_MAX));
    }

    private static OptionalDouble mergeAvg(OptionalDouble first, OptionalLong firstRowCount, OptionalDouble second, OptionalLong secondRowCount)
    {
        if (first.isPresent() && second.isPresent() && firstRowCount.isPresent() && secondRowCount.isPresent()) {
            double sumFirst = first.getAsDouble() * firstRowCount.getAsLong();
            double sumSecond = second.getAsDouble() * secondRowCount.getAsLong();
            long totalRowCount = firstRowCount.getAsLong() + secondRowCount.getAsLong();
            return OptionalDouble.of((sumFirst + sumSecond) / totalRowCount);
        }
        return OptionalDouble.empty();
    }

    public static Set<ColumnStatisticType> getSupportedStatistics(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        else if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, MIN, MAX, NUMBER_OF_DISTINCT_VALUES);
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, MIN, MAX, NUMBER_OF_DISTINCT_VALUES);
        }
        else if (isVarcharType(type)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, MAX_VALUE_SIZE_IN_BYTES, AVERAGE_VALUE_SIZE_IN_BYTES);
        }
        else if (isCharType(type)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES);
        }
        else if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, MAX_VALUE_SIZE_IN_BYTES, AVERAGE_VALUE_SIZE_IN_BYTES);
        }
        else if (type.equals(DATE) || type.equals(TIMESTAMP)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, MIN, MAX, NUMBER_OF_DISTINCT_VALUES);
        }
        else if (type instanceof DecimalType) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, MIN, MAX, NUMBER_OF_DISTINCT_VALUES);
        }
        else {
            return ImmutableSet.of();
        }
    }

    public static Object getMinMaxAsPrestoTypeValue(Object value, Type prestoType, DateTimeZone timeZone)
    {
        requireNonNull(value, "high/low value connot be null");

        if (prestoType.equals(BIGINT) || prestoType.equals(INTEGER) || prestoType.equals(SMALLINT) || prestoType.equals(TINYINT)) {
            checkArgument(value instanceof Long, "expected Long value but got " + value.getClass());
            return value;
        }
        if (prestoType.equals(DOUBLE)) {
            checkArgument(value instanceof Double, "expected Double value but got " + value.getClass());
            return value;
        }
        if (prestoType.equals(REAL)) {
            checkArgument(value instanceof Double, "expected Double value but got " + value.getClass());
            return (long) floatToRawIntBits((float) (double) value);
        }
        if (prestoType.equals(DATE)) {
            checkArgument(value instanceof LocalDate, "expected LocalDate value but got " + value.getClass());
            return ((LocalDate) value).toEpochDay();
        }
        if (prestoType.equals(TIMESTAMP)) {
            checkArgument(value instanceof Long, "expected Long value but got " + value.getClass());
            return timeZone.convertLocalToUTC((long) value * 1000, false);
        }
        if (prestoType instanceof DecimalType) {
            checkArgument(value instanceof BigDecimal, "expected BigDecimal value but got " + value.getClass());
            BigInteger unscaled = Decimals.rescale((BigDecimal) value, (DecimalType) prestoType).unscaledValue();
            if (Decimals.isShortDecimal(prestoType)) {
                return unscaled.longValueExact();
            }
            else {
                return Decimals.encodeUnscaledValue(unscaled);
            }
        }

        throw new IllegalArgumentException("Unsupported presto type " + prestoType);
    }

    public static PartitionStatistics migrateStatistics(PartitionStatistics statistics, String oldColumnName, String newColumnName)
    {
        return new PartitionStatistics(statistics.getBasicStatistics(), migrateStatistics(statistics.getColumnStatistics(), oldColumnName, newColumnName));
    }

    public static Map<String, HiveColumnStatistics> migrateStatistics(Map<String, HiveColumnStatistics> statistics, String oldColumnName, String newColumnName)
    {
        return statistics.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().equals(oldColumnName) ? newColumnName : entry.getKey(), Entry::getValue));
    }

    public static PartitionStatistics removeStatistics(PartitionStatistics statistics, String column)
    {
        return new PartitionStatistics(statistics.getBasicStatistics(), removeStatistics(statistics.getColumnStatistics(), column));
    }

    public static Map<String, HiveColumnStatistics> removeStatistics(Map<String, HiveColumnStatistics> statistics, String column)
    {
        return statistics.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(column))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static Map<List<String>, ComputedStatistics> groupComputedStatisticsByPartition(
            List<ComputedStatistics> computedStatistics, List<String> partitionColumns, Map<String, Type> columnTypes)
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
                "Unexpected groping. Partition columns: %s. Grouping columns: %s", partitionColumns, statistics.getGroupingColumns());
        Page partitionColumnsPage = new Page(1, statistics.getGropingValues().toArray(new Block[] {}));
        return createPartitionValues(partitionColumnTypes, partitionColumnsPage, 0);
    }

    public static Map<String, HiveColumnStatistics> fromComputedStatistics(
            ConnectorSession session, DateTimeZone timeZone, Map<ColumnStatisticMetadata, Block> computedStatistics, Map<String, Type> columnTypes, long rowCount)
    {
        return groupByColumn(computedStatistics)
                .entrySet()
                .stream()
                .collect(toImmutableMap(Entry::getKey, entry -> createHiveColumnStatistics(session, timeZone, entry.getValue(), columnTypes.get(entry.getKey()), rowCount)));
    }

    private static Map<String, Map<ColumnStatisticType, Block>> groupByColumn(Map<ColumnStatisticMetadata, Block> computedStatistics)
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

        // MIN MAX
        if (computedStatistics.containsKey(MIN)) {
            Block block = computedStatistics.get(MIN);
            if (!block.isNull(0)) {
                result.setLowValue(getMinMaxAsMetastoreValue(session, timeZone, columnType, block));
            }
        }
        if (computedStatistics.containsKey(MAX)) {
            Block block = computedStatistics.get(MAX);
            if (!block.isNull(0)) {
                result.setHighValue(getMinMaxAsMetastoreValue(session, timeZone, columnType, block));
            }
        }

        // NDV
        if (computedStatistics.containsKey(NUMBER_OF_DISTINCT_VALUES)) {
            result.setDistinctValuesCount(BigintType.BIGINT.getLong(computedStatistics.get(NUMBER_OF_DISTINCT_VALUES), 0));
        }

        // DATA SIZE
        if (computedStatistics.containsKey(MAX_VALUE_SIZE_IN_BYTES)) {
            result.setMaxColumnLength(BigintType.BIGINT.getLong(computedStatistics.get(MAX_VALUE_SIZE_IN_BYTES), 0));
        }
        if (computedStatistics.containsKey(AVERAGE_VALUE_SIZE_IN_BYTES)) {
            result.setAverageColumnLength(DOUBLE.getDouble(computedStatistics.get(AVERAGE_VALUE_SIZE_IN_BYTES), 0));
        }

        // NUMBER OF NULLS
        if (computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            result.setNullsCount(rowCount - BigintType.BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0));
        }

        // NUMBER OF FALSE, NUMBER OF TRUE
        if (computedStatistics.containsKey(NUMBER_OF_TRUE_VALUES)) {
            long numberOfTrue = BigintType.BIGINT.getLong(computedStatistics.get(NUMBER_OF_TRUE_VALUES), 0);
            result.setTrueCount(numberOfTrue);
            if (computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
                long numberOfNonNullValues = BigintType.BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0);
                result.setFalseCount(numberOfNonNullValues - numberOfTrue);
            }
        }
        return result.build();
    }

    private static Comparable<?> getMinMaxAsMetastoreValue(ConnectorSession session, DateTimeZone timeZone, Type type, Block block)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return ((Number) type.getObjectValue(session, block, 0)).longValue();
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            return ((Number) type.getObjectValue(session, block, 0)).doubleValue();
        }
        else if (type.equals(DATE)) {
            return LocalDate.ofEpochDay(((SqlDate) type.getObjectValue(session, block, 0)).getDays());
        }
        else if (type.equals(TIMESTAMP)) {
            long valueUtc = block.getLong(0, 0);
            return timeZone.convertUTCToLocal(valueUtc) / 1000;
        }
        else if (type instanceof DecimalType) {
            return ((SqlDecimal) type.getObjectValue(session, block, 0)).toBigDecimal();
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    private static OptionalLong reduce(OptionalLong first, OptionalLong second, ReduceOperator operator)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT:
                    return OptionalLong.of(first.getAsLong() - second.getAsLong());
                case SELECT_MAX:
                    return OptionalLong.of(max(first.getAsLong(), second.getAsLong()));
                case SELECT_MIN:
                    return OptionalLong.of(min(first.getAsLong(), second.getAsLong()));
                case AVG:
                    return OptionalLong.of((first.getAsLong() + second.getAsLong()) / 2);
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        return OptionalLong.empty();
    }

    private static OptionalDouble reduce(OptionalDouble first, OptionalDouble second, ReduceOperator operator)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalDouble.of(first.getAsDouble() + second.getAsDouble());
                case SUBTRACT:
                    return OptionalDouble.of(first.getAsDouble() - second.getAsDouble());
                case SELECT_MAX:
                    return OptionalDouble.of(max(first.getAsDouble(), second.getAsDouble()));
                case SELECT_MIN:
                    return OptionalDouble.of(min(first.getAsDouble(), second.getAsDouble()));
                case AVG:
                    return OptionalDouble.of((first.getAsDouble() + second.getAsDouble()) / 2);
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        return OptionalDouble.empty();
    }

    @SuppressWarnings("unchecked")
    private static Optional<? extends Comparable<?>> reduce(
            Optional<? extends Comparable<?>> firstOptional,
            Optional<? extends Comparable<?>> secondOptional,
            ReduceOperator operator)
    {
        if (firstOptional.isPresent() && secondOptional.isPresent()) {
            Comparable<?> first = firstOptional.get();
            Comparable<?> second = secondOptional.get();
            checkArgument(first.getClass().equals(second.getClass()), "cannot compare comparable of different types: %s != %s", first.getClass(), second.getClass());
            switch (operator) {
                case SELECT_MAX:
                    return Optional.of(max((Comparable) first, (Comparable) second));
                case SELECT_MIN:
                    return Optional.of(min((Comparable) first, (Comparable) second));
                default:
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
        }
        return Optional.empty();
    }

    private static <T extends Comparable<T>> T max(T first, T second)
    {
        return first.compareTo(second) > 0 ? first : second;
    }

    private static <T extends Comparable<T>> T min(T first, T second)
    {
        return first.compareTo(second) < 0 ? first : second;
    }

    public enum ReduceOperator
    {
        ADD,
        SUBTRACT,
        SELECT_MIN,
        SELECT_MAX,
        AVG
    }
}
