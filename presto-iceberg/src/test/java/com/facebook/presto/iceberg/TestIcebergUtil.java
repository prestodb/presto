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
import static com.facebook.presto.iceberg.IcebergUtil.getNextValue;
import static com.facebook.presto.iceberg.IcebergUtil.getPreviousValue;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergUtil
{
    @Test
    public void testPreviousValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getPreviousValue(BIGINT, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(BIGINT, minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(getPreviousValue(BIGINT, 1234L))
                .isEqualTo(Optional.of(1233L));

        assertThat(getPreviousValue(BIGINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(BIGINT, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getNextValue(BIGINT, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(BIGINT, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getNextValue(BIGINT, 1234L))
                .isEqualTo(Optional.of(1235L));

        assertThat(getNextValue(BIGINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(BIGINT, maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getPreviousValue(INTEGER, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(INTEGER, minValue + 1))
                .isEqualTo(Optional.of(minValue));
        assertThat(getPreviousValue(DATE, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(DATE, minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(getPreviousValue(INTEGER, 1234L))
                .isEqualTo(Optional.of(1233L));
        assertThat(getPreviousValue(DATE, 1234L))
                .isEqualTo(Optional.of(1233L));

        assertThat(getPreviousValue(INTEGER, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(INTEGER, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getPreviousValue(DATE, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(DATE, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getNextValue(INTEGER, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(INTEGER, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getNextValue(DATE, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(DATE, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getNextValue(INTEGER, 1234L))
                .isEqualTo(Optional.of(1235L));
        assertThat(getNextValue(DATE, 1234L))
                .isEqualTo(Optional.of(1235L));

        assertThat(getNextValue(INTEGER, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(INTEGER, maxValue))
                .isEqualTo(Optional.empty());
        assertThat(getNextValue(DATE, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(DATE, maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getPreviousValue(TIMESTAMP, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(TIMESTAMP, minValue + 1))
                .isEqualTo(Optional.of(minValue));
        assertThat(getPreviousValue(TIMESTAMP_MICROSECONDS, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(TIMESTAMP_MICROSECONDS, minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(getPreviousValue(TIMESTAMP, 1234L))
                .isEqualTo(Optional.of(1233L));
        assertThat(getPreviousValue(TIMESTAMP_MICROSECONDS, 1234L))
                .isEqualTo(Optional.of(1233L));

        assertThat(getPreviousValue(TIMESTAMP, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(TIMESTAMP, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getPreviousValue(TIMESTAMP_MICROSECONDS, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(TIMESTAMP_MICROSECONDS, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getNextValue(TIMESTAMP, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(TIMESTAMP, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getNextValue(TIMESTAMP_MICROSECONDS, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(TIMESTAMP_MICROSECONDS, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getNextValue(TIMESTAMP, 1234L))
                .isEqualTo(Optional.of(1235L));
        assertThat(getNextValue(TIMESTAMP_MICROSECONDS, 1234L))
                .isEqualTo(Optional.of(1235L));

        assertThat(getNextValue(TIMESTAMP, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(TIMESTAMP, maxValue))
                .isEqualTo(Optional.empty());
        assertThat(getNextValue(TIMESTAMP_MICROSECONDS, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(TIMESTAMP_MICROSECONDS, maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getPreviousValue(SMALLINT, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(SMALLINT, minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(getPreviousValue(SMALLINT, 1234L))
                .isEqualTo(Optional.of(1233L));

        assertThat(getPreviousValue(SMALLINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(SMALLINT, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getNextValue(SMALLINT, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(SMALLINT, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getNextValue(SMALLINT, 1234L))
                .isEqualTo(Optional.of(1235L));

        assertThat(getNextValue(SMALLINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(SMALLINT, maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getPreviousValue(TINYINT, minValue))
                .isEqualTo(Optional.empty());
        assertThat(getPreviousValue(TINYINT, minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(getPreviousValue(TINYINT, 123L))
                .isEqualTo(Optional.of(122L));

        assertThat(getPreviousValue(TINYINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getPreviousValue(TINYINT, maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getNextValue(TINYINT, minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getNextValue(TINYINT, minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getNextValue(TINYINT, 123L))
                .isEqualTo(Optional.of(124L));

        assertThat(getNextValue(TINYINT, maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getNextValue(TINYINT, maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousAndNextValueForDouble()
    {
        assertThat(getPreviousValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getPreviousValue(DOUBLE, getNextValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE).get()).get()))
                .isEqualTo(Double.NEGATIVE_INFINITY);

        assertThat(getPreviousValue(DOUBLE, DOUBLE_POSITIVE_ZERO))
                .isEqualTo(getPreviousValue(DOUBLE, DOUBLE_NEGATIVE_ZERO));
        assertThat(longBitsToDouble((long) getPreviousValue(DOUBLE, getNextValue(DOUBLE, DOUBLE_NEGATIVE_ZERO).get()).get()))
                .isEqualTo(0.0d);
        assertThat(getPreviousValue(DOUBLE, getNextValue(DOUBLE, DOUBLE_NEGATIVE_ZERO).get()).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);
        assertThat(longBitsToDouble((long) getNextValue(DOUBLE, getPreviousValue(DOUBLE, DOUBLE_POSITIVE_ZERO).get()).get()))
                .isEqualTo(0.0d);
        assertThat(getNextValue(DOUBLE, getPreviousValue(DOUBLE, DOUBLE_POSITIVE_ZERO).get()).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);

        assertThat(getNextValue(DOUBLE, DOUBLE_POSITIVE_INFINITE))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getNextValue(DOUBLE, getPreviousValue(DOUBLE, DOUBLE_POSITIVE_INFINITE).get()).get()))
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousAndNextValueForReal()
    {
        assertThat(getPreviousValue(REAL, REAL_NEGATIVE_INFINITE))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getPreviousValue(REAL, getNextValue(REAL, REAL_NEGATIVE_INFINITE).get()).get()))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(getPreviousValue(REAL, REAL_POSITIVE_ZERO))
                .isEqualTo(getPreviousValue(REAL, REAL_NEGATIVE_ZERO));
        assertThat(intBitsToFloat((int) getPreviousValue(REAL, getNextValue(REAL, REAL_NEGATIVE_ZERO).get()).get()))
                .isEqualTo(0.0f);
        assertThat(getPreviousValue(REAL, getNextValue(REAL, REAL_NEGATIVE_ZERO).get()).get())
                .isEqualTo(REAL_POSITIVE_ZERO);
        assertThat(intBitsToFloat((int) getNextValue(REAL, getPreviousValue(REAL, REAL_POSITIVE_ZERO).get()).get()))
                .isEqualTo(0.0f);
        assertThat(getNextValue(REAL, getPreviousValue(REAL, REAL_POSITIVE_ZERO).get()).get())
                .isEqualTo(REAL_POSITIVE_ZERO);

        assertThat(getNextValue(REAL, REAL_POSITIVE_INFINITE))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getNextValue(REAL, getPreviousValue(REAL, REAL_POSITIVE_INFINITE).get()).get()))
                .isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousValueForOtherType()
    {
        assertThat(getPreviousValue(VARCHAR, "anystr"))
                .isEmpty();
        assertThat(getPreviousValue(BOOLEAN, true))
                .isEmpty();
        assertThat(getPreviousValue(TIME, 123L))
                .isEmpty();
        assertThat(getPreviousValue(DecimalType.createDecimalType(8, 2), 12345L))
                .isEmpty();
        assertThat(getPreviousValue(DecimalType.createDecimalType(20, 2),
                    encodeScaledValue(new BigDecimal(111111111111111123.45))))
                .isEmpty();
    }

    @Test
    public void testNextValueForOtherType()
    {
        assertThat(getNextValue(VARCHAR, "anystr"))
                .isEmpty();
        assertThat(getNextValue(BOOLEAN, true))
                .isEmpty();
        assertThat(getNextValue(TIME, 123L))
                .isEmpty();
        assertThat(getNextValue(DecimalType.createDecimalType(8, 2), 12345L))
                .isEmpty();
        assertThat(getNextValue(DecimalType.createDecimalType(20, 2),
                    encodeScaledValue(new BigDecimal(111111111111111123.45))))
                .isEmpty();
    }
}
