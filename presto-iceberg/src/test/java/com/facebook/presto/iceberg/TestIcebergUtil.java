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

import com.facebook.presto.common.type.DecimalType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_NEGATIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_NEGATIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_POSITIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_POSITIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_NEGATIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_NEGATIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_POSITIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_POSITIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.getAdjacentValue;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergUtil
{
    @Test
    public void testPreviousValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(BIGINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(BIGINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(BIGINT, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(BIGINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(BIGINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(BIGINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(BIGINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(BIGINT, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(BIGINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(BIGINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getAdjacentValue(INTEGER, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(INTEGER, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));
        assertThat(getAdjacentValue(DATE, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(DATE, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(INTEGER, 1234L, true))
                .isEqualTo(Optional.of(1233L));
        assertThat(getAdjacentValue(DATE, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(INTEGER, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(INTEGER, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getAdjacentValue(DATE, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(DATE, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getAdjacentValue(INTEGER, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(INTEGER, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getAdjacentValue(DATE, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(DATE, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(INTEGER, 1234L, false))
                .isEqualTo(Optional.of(1235L));
        assertThat(getAdjacentValue(DATE, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(INTEGER, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(INTEGER, maxValue, false))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(DATE, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(DATE, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(TIMESTAMP, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(TIMESTAMP, 1234L, true))
                .isEqualTo(Optional.of(1233L));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(TIMESTAMP, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TIMESTAMP, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(TIMESTAMP, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TIMESTAMP, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(TIMESTAMP, 1234L, false))
                .isEqualTo(Optional.of(1235L));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(TIMESTAMP, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TIMESTAMP, maxValue, false))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getAdjacentValue(SMALLINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(SMALLINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(SMALLINT, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(SMALLINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(SMALLINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getAdjacentValue(SMALLINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(SMALLINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(SMALLINT, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(SMALLINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(SMALLINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getAdjacentValue(TINYINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TINYINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(TINYINT, 123L, true))
                .isEqualTo(Optional.of(122L));

        assertThat(getAdjacentValue(TINYINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TINYINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getAdjacentValue(TINYINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TINYINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(TINYINT, 123L, false))
                .isEqualTo(Optional.of(124L));

        assertThat(getAdjacentValue(TINYINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TINYINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousAndNextValueForDouble()
    {
        assertThat(getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE, true))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE, false).get(), true).get()))
                .isEqualTo(Double.NEGATIVE_INFINITY);

        assertThat(getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true))
                .isEqualTo(getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, true));
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, false).get(), true).get()))
                .isEqualTo(0.0d);
        assertThat(getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, false).get(), true).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true).get(), false).get()))
                .isEqualTo(0.0d);
        assertThat(getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true).get(), false).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);

        assertThat(getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_INFINITE, false))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_INFINITE, true).get(), false).get()))
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousAndNextValueForReal()
    {
        assertThat(getAdjacentValue(REAL, REAL_NEGATIVE_INFINITE, true))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_INFINITE, false).get(), true).get()))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true))
                .isEqualTo(getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, true));
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, false).get(), true).get()))
                .isEqualTo(0.0f);
        assertThat(getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, false).get(), true).get())
                .isEqualTo(REAL_POSITIVE_ZERO);
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true).get(), false).get()))
                .isEqualTo(0.0f);
        assertThat(getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true).get(), false).get())
                .isEqualTo(REAL_POSITIVE_ZERO);

        assertThat(getAdjacentValue(REAL, REAL_POSITIVE_INFINITE, false))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_INFINITE, true).get(), false).get()))
                .isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousValueForOtherType()
    {
        assertThat(getAdjacentValue(VARCHAR, "anystr", true))
                .isEmpty();
        assertThat(getAdjacentValue(BOOLEAN, true, true))
                .isEmpty();
        assertThat(getAdjacentValue(TIME, 123L, true))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(8, 2), 12345L, true))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(20, 2),
                encodeScaledValue(new BigDecimal(111111111111111123.45)), true))
                .isEmpty();
    }

    @Test
    public void testNextValueForOtherType()
    {
        assertThat(getAdjacentValue(VARCHAR, "anystr", false))
                .isEmpty();
        assertThat(getAdjacentValue(BOOLEAN, true, false))
                .isEmpty();
        assertThat(getAdjacentValue(TIME, 123L, false))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(8, 2), 12345L, false))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(20, 2),
                encodeScaledValue(new BigDecimal(111111111111111123.45)), false))
                .isEmpty();
    }

    @Test
    public void testGetTargetSplitSize()
    {
        assertEquals(1024, getTargetSplitSize(1024, 512).toBytes());
        assertEquals(512, getTargetSplitSize(0, 512).toBytes());
    }
}
