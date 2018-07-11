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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public final class Statistics
{
    private Statistics() {}

    public static PartitionStatistics merge(PartitionStatistics first, PartitionStatistics second)
    {
        return new PartitionStatistics(
                reduce(first.getBasicStatistics(), second.getBasicStatistics(), ADD),
                ImmutableMap.of());
    }

    public static HiveBasicStatistics reduce(HiveBasicStatistics first, HiveBasicStatistics second, ReduceOperator operator)
    {
        return new HiveBasicStatistics(
                reduce(first.getFileCount(), second.getFileCount(), operator),
                reduce(first.getRowCount(), second.getRowCount(), operator),
                reduce(first.getInMemoryDataSizeInBytes(), second.getInMemoryDataSizeInBytes(), operator),
                reduce(first.getOnDiskDataSizeInBytes(), second.getOnDiskDataSizeInBytes(), operator));
    }

    private static OptionalLong reduce(OptionalLong first, OptionalLong second, ReduceOperator operator)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT:
                    return OptionalLong.of(first.getAsLong() - second.getAsLong());
            }
        }
        return OptionalLong.empty();
    }

    public static Range getMinMaxAsPrestoNativeValues(HiveColumnStatistics statistics, Type type, DateTimeZone timeZone)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return statistics.getIntegerStatistics().map(integerStatistics -> Range.create(
                    integerStatistics.getMin(),
                    integerStatistics.getMax()))
                    .orElse(Range.empty());
        }
        if (type.equals(DOUBLE)) {
            return statistics.getDoubleStatistics().map(doubleStatistics -> Range.create(
                    doubleStatistics.getMin(),
                    doubleStatistics.getMax()))
                    .orElse(Range.empty());
        }
        if (type.equals(REAL)) {
            return statistics.getDoubleStatistics().map(doubleStatistics -> Range.create(
                    boxed(doubleStatistics.getMin()).map(Statistics::floatAsDoubleToLongBits),
                    boxed(doubleStatistics.getMax()).map(Statistics::floatAsDoubleToLongBits)))
                    .orElse(Range.empty());
        }
        if (type.equals(DATE)) {
            return statistics.getDateStatistics().map(dateStatistics -> Range.create(
                    dateStatistics.getMin().map(LocalDate::toEpochDay),
                    dateStatistics.getMax().map(LocalDate::toEpochDay)))
                    .orElse(Range.empty());
        }
        if (type.equals(TIMESTAMP)) {
            return statistics.getIntegerStatistics().map(integerStatistics -> Range.create(
                    boxed(integerStatistics.getMin()).map(value -> convertLocalToUtc(timeZone, value)),
                    boxed(integerStatistics.getMax()).map(value -> convertLocalToUtc(timeZone, value))))
                    .orElse(Range.empty());
        }
        if (type instanceof DecimalType) {
            return statistics.getDecimalStatistics().map(decimalStatistics -> Range.create(
                    decimalStatistics.getMin().map(value -> encodeDecimal(type, value)),
                    decimalStatistics.getMax().map(value -> encodeDecimal(type, value))))
                    .orElse(Range.empty());
        }
        return Range.empty();
    }

    private static long floatAsDoubleToLongBits(double value)
    {
        return floatToRawIntBits((float) value);
    }

    private static long convertLocalToUtc(DateTimeZone timeZone, long value)
    {
        return timeZone.convertLocalToUTC(value * 1000, false);
    }

    private static Comparable<?> encodeDecimal(Type type, BigDecimal value)
    {
        BigInteger unscaled = Decimals.rescale(value, (DecimalType) type).unscaledValue();
        if (Decimals.isShortDecimal(type)) {
            return unscaled.longValueExact();
        }
        return Decimals.encodeUnscaledValue(unscaled);
    }

    private static Optional<Long> boxed(OptionalLong input)
    {
        return input.isPresent() ? Optional.of(input.getAsLong()) : Optional.empty();
    }

    private static Optional<Double> boxed(OptionalDouble input)
    {
        return input.isPresent() ? Optional.of(input.getAsDouble()) : Optional.empty();
    }

    public enum ReduceOperator
    {
        ADD,
        SUBTRACT,
    }

    public static class Range
    {
        private static final Range EMPTY = new Range(Optional.empty(), Optional.empty());

        private final Optional<? extends Comparable<?>> min;
        private final Optional<? extends Comparable<?>> max;

        public static Range empty()
        {
            return EMPTY;
        }

        public static Range create(Optional<? extends Comparable<?>> min, Optional<? extends Comparable<?>> max)
        {
            return new Range(min, max);
        }

        public static Range create(OptionalLong min, OptionalLong max)
        {
            return new Range(boxed(min), boxed(max));
        }

        public static Range create(OptionalDouble min, OptionalDouble max)
        {
            return new Range(boxed(min), boxed(max));
        }

        public Range(Optional<? extends Comparable<?>> min, Optional<? extends Comparable<?>> max)
        {
            this.min = requireNonNull(min, "min is null");
            this.max = requireNonNull(max, "max is null");
        }

        public Optional<? extends Comparable<?>> getMin()
        {
            return min;
        }

        public Optional<? extends Comparable<?>> getMax()
        {
            return max;
        }
    }
}
