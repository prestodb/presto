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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
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
import static com.google.common.base.Preconditions.checkArgument;
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

    public static boolean isMinMaxSupportedForType(Type type)
    {
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type.equals(DATE)
                || type.equals(TIMESTAMP)
                || (type instanceof DecimalType);
    }

    public static Object getMinMaxAsPrestoTypeValue(Object value, Type prestoType, DateTimeZone timeZone)
    {
        checkArgument(isMinMaxSupportedForType(prestoType), "Unsupported type " + prestoType);
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

    public enum ReduceOperator
    {
        ADD,
        SUBTRACT,
    }
}
