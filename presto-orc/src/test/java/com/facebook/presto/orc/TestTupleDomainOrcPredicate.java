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
package com.facebook.presto.orc;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.statistics.BinaryColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.notNull;
import static com.facebook.presto.common.predicate.Domain.onlyNull;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThanOrEqual;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.TupleDomainOrcPredicate.getDomain;
import static com.facebook.presto.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static org.testng.Assert.assertEquals;

public class TestTupleDomainOrcPredicate
{
    private static final Type SHORT_DECIMAL = createDecimalType(5, 2);
    private static final Type LONG_DECIMAL = createDecimalType(20, 10);
    private static final Type CHAR = createCharType(10);

    @Test
    public void testBoolean()
    {
        assertEquals(getDomain(BOOLEAN, 0, null), Domain.none(BOOLEAN));
        assertEquals(getDomain(BOOLEAN, 10, null), Domain.all(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(null, null)), Domain.none(BOOLEAN));
        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(0L, null)), Domain.none(BOOLEAN));
        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(0L, 0L)), Domain.none(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(0L, 0L)), onlyNull(BOOLEAN));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, null)), notNull(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 10L)), singleValue(BOOLEAN, true));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 0L)), singleValue(BOOLEAN, false));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 5L)), Domain.all(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 10L)), create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), true));
        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 0L)), create(ValueSet.ofRanges(Range.equal(BOOLEAN, false)), true));
    }

    private static ColumnStatistics booleanColumnStats(Long numberOfValues, Long trueValueCount)
    {
        BooleanStatistics booleanStatistics = null;
        if (trueValueCount != null) {
            booleanStatistics = new BooleanStatistics(trueValueCount);
            return new BooleanColumnStatistics(numberOfValues, null, booleanStatistics);
        }
        return new ColumnStatistics(numberOfValues, null);
    }

    @Test
    public void testBigint()
    {
        assertEquals(getDomain(BIGINT, 0, null), Domain.none(BIGINT));
        assertEquals(getDomain(BIGINT, 10, null), Domain.all(BIGINT));

        assertEquals(getDomain(BIGINT, 0, integerColumnStats(null, null, null)), Domain.none(BIGINT));
        assertEquals(getDomain(BIGINT, 0, integerColumnStats(0L, null, null)), Domain.none(BIGINT));
        assertEquals(getDomain(BIGINT, 0, integerColumnStats(0L, 100L, 100L)), Domain.none(BIGINT));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(0L, null, null)), onlyNull(BIGINT));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, null, null)), notNull(BIGINT));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 100L, 100L)), singleValue(BIGINT, 100L));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, 100L)), create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), false));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, null, 100L)), create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 100L)), false));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, null)), create(ValueSet.ofRanges(greaterThanOrEqual(BIGINT, 0L)), false));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, 100L)), create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), true));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, null, 100L)), create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 100L)), true));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, null)), create(ValueSet.ofRanges(greaterThanOrEqual(BIGINT, 0L)), true));
    }

    private static IntegerColumnStatistics integerColumnStats(Long numberOfValues, Long minimum, Long maximum)
    {
        return new IntegerColumnStatistics(numberOfValues, null, new IntegerStatistics(minimum, maximum, null));
    }

    @Test
    public void testDouble()
    {
        assertEquals(getDomain(DOUBLE, 0, null), Domain.none(DOUBLE));
        assertEquals(getDomain(DOUBLE, 10, null), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(null, null, null)), Domain.none(DOUBLE));
        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(0L, null, null)), Domain.none(DOUBLE));
        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(0L, 42.24, 42.24)), Domain.none(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(0L, null, null)), onlyNull(DOUBLE));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, null)), notNull(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 42.24, 42.24)), singleValue(DOUBLE, 42.24));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, 42.24)), create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), false));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, 42.24)), create(ValueSet.ofRanges(lessThanOrEqual(DOUBLE, 42.24)), false));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, null)), create(ValueSet.ofRanges(greaterThanOrEqual(DOUBLE, 3.3)), false));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, 42.24)), create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), true));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, null, 42.24)), create(ValueSet.ofRanges(lessThanOrEqual(DOUBLE, 42.24)), true));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, null)), create(ValueSet.ofRanges(greaterThanOrEqual(DOUBLE, 3.3)), true));
    }

    private static DoubleColumnStatistics doubleColumnStats(Long numberOfValues, Double minimum, Double maximum)
    {
        return new DoubleColumnStatistics(numberOfValues, null, new DoubleStatistics(minimum, maximum));
    }

    @Test
    public void testFloat()
    {
        assertEquals(getDomain(REAL, 0, null), Domain.none(REAL));
        assertEquals(getDomain(REAL, 10, null), Domain.all(REAL));

        assertEquals(getDomain(REAL, 0, doubleColumnStats(null, null, null)), Domain.none(REAL));
        assertEquals(getDomain(REAL, 0, doubleColumnStats(0L, null, null)), Domain.none(REAL));
        assertEquals(getDomain(REAL, 0, doubleColumnStats(0L, (double) 42.24f, (double) 42.24f)), Domain.none(REAL));

        assertEquals(getDomain(REAL, 10, doubleColumnStats(0L, null, null)), onlyNull(REAL));
        assertEquals(getDomain(REAL, 10, doubleColumnStats(10L, null, null)), notNull(REAL));

        assertEquals(getDomain(REAL, 10, doubleColumnStats(10L, (double) 42.24f, (double) 42.24f)), singleValue(REAL, (long) floatToRawIntBits(42.24f)));

        assertEquals(getDomain(REAL, 10, doubleColumnStats(10L, 3.3, (double) 42.24f)), create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(3.3f), true, (long) floatToRawIntBits(42.24f), true)), false));
        assertEquals(getDomain(REAL, 10, doubleColumnStats(10L, null, (double) 42.24f)), create(ValueSet.ofRanges(lessThanOrEqual(REAL, (long) floatToRawIntBits(42.24f))), false));
        assertEquals(getDomain(REAL, 10, doubleColumnStats(10L, 3.3, null)), create(ValueSet.ofRanges(greaterThanOrEqual(REAL, (long) floatToRawIntBits(3.3f))), false));

        assertEquals(getDomain(REAL, 10, doubleColumnStats(5L, 3.3, (double) 42.24f)), create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(3.3f), true, (long) floatToRawIntBits(42.24f), true)), true));
        assertEquals(getDomain(REAL, 10, doubleColumnStats(5L, null, (double) 42.24f)), create(ValueSet.ofRanges(lessThanOrEqual(REAL, (long) floatToRawIntBits(42.24f))), true));
        assertEquals(getDomain(REAL, 10, doubleColumnStats(5L, 3.3, null)), create(ValueSet.ofRanges(greaterThanOrEqual(REAL, (long) floatToRawIntBits(3.3f))), true));
    }

    @Test
    public void testString()
    {
        assertEquals(getDomain(VARCHAR, 0, null), Domain.none(VARCHAR));
        assertEquals(getDomain(VARCHAR, 10, null), Domain.all(VARCHAR));

        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(null, null, null)), Domain.none(VARCHAR));
        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(0L, null, null)), Domain.none(VARCHAR));
        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(0L, "taco", "taco")), Domain.none(VARCHAR));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(0L, null, null)), onlyNull(VARCHAR));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, null, null)), notNull(VARCHAR));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "taco", "taco")), singleValue(VARCHAR, utf8Slice("taco")));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", "taco")), create(ValueSet.ofRanges(range(VARCHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, null, "taco")), create(ValueSet.ofRanges(lessThanOrEqual(VARCHAR, utf8Slice("taco"))), false));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", null)), create(ValueSet.ofRanges(greaterThanOrEqual(VARCHAR, utf8Slice("apple"))), false));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", "taco")), create(ValueSet.ofRanges(range(VARCHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, null, "taco")), create(ValueSet.ofRanges(lessThanOrEqual(VARCHAR, utf8Slice("taco"))), true));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", null)), create(ValueSet.ofRanges(greaterThanOrEqual(VARCHAR, utf8Slice("apple"))), true));
    }

    @Test
    public void testChar()
    {
        assertEquals(getDomain(CHAR, 0, null), Domain.none(CHAR));
        assertEquals(getDomain(CHAR, 10, null), Domain.all(CHAR));

        assertEquals(getDomain(CHAR, 0, stringColumnStats(null, null, null)), Domain.none(CHAR));
        assertEquals(getDomain(CHAR, 0, stringColumnStats(0L, null, null)), Domain.none(CHAR));
        assertEquals(getDomain(CHAR, 0, stringColumnStats(0L, "taco      ", "taco      ")), Domain.none(CHAR));
        assertEquals(getDomain(CHAR, 0, stringColumnStats(0L, "taco", "taco      ")), Domain.none(CHAR));

        assertEquals(getDomain(CHAR, 10, stringColumnStats(0L, null, null)), onlyNull(CHAR));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, null, null)), notNull(CHAR));

        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "taco      ", "taco      ")), singleValue(CHAR, utf8Slice("taco")));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "taco", "taco      ")), singleValue(CHAR, utf8Slice("taco")));

        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", "taco      ")), create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", "taco")), create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, null, "taco      ")), create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), false));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, null, "taco")), create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), false));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", null)), create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), false));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "apple", null)), create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), false));

        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", "taco      ")), create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", "taco")), create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, null, "taco      ")), create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), true));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, null, "taco")), create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), true));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", null)), create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), true));
        assertEquals(getDomain(CHAR, 10, stringColumnStats(5L, "apple", null)), create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), true));

        assertEquals(getDomain(CHAR, 10, stringColumnStats(10L, "\0 ", " ")), create(ValueSet.ofRanges(range(CHAR, utf8Slice("\0"), true, utf8Slice(""), true)), false));
    }

    private static ColumnStatistics stringColumnStats(Long numberOfValues, String minimum, String maximum)
    {
        Slice minimumSlice = minimum == null ? null : utf8Slice(minimum);
        Slice maximumSlice = maximum == null ? null : utf8Slice(maximum);
        // sum and minAverageValueSizeInBytes are not used in this test; they could be arbitrary numbers
        return new StringColumnStatistics(numberOfValues, null, new StringStatistics(minimumSlice, maximumSlice, 100L));
    }

    @Test
    public void testDate()
    {
        assertEquals(getDomain(DATE, 0, null), Domain.none(DATE));
        assertEquals(getDomain(DATE, 10, null), Domain.all(DATE));

        assertEquals(getDomain(DATE, 0, dateColumnStats(null, null, null)), Domain.none(DATE));
        assertEquals(getDomain(DATE, 0, dateColumnStats(0L, null, null)), Domain.none(DATE));
        assertEquals(getDomain(DATE, 0, dateColumnStats(0L, 100, 100)), Domain.none(DATE));

        assertEquals(getDomain(DATE, 10, dateColumnStats(0L, null, null)), onlyNull(DATE));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, null, null)), notNull(DATE));

        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 100, 100)), singleValue(DATE, 100L));

        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 0, 100)), create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), false));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, null, 100)), create(ValueSet.ofRanges(lessThanOrEqual(DATE, 100L)), false));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 0, null)), create(ValueSet.ofRanges(greaterThanOrEqual(DATE, 0L)), false));

        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, 0, 100)), create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), true));
        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, null, 100)), create(ValueSet.ofRanges(lessThanOrEqual(DATE, 100L)), true));
        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, 0, null)), create(ValueSet.ofRanges(greaterThanOrEqual(DATE, 0L)), true));
    }

    private static ColumnStatistics dateColumnStats(Long numberOfValues, Integer minimum, Integer maximum)
    {
        return new DateColumnStatistics(numberOfValues, null, new DateStatistics(minimum, maximum));
    }

    @Test
    public void testDecimal()
    {
        assertEquals(getDomain(SHORT_DECIMAL, 0, null), Domain.none(SHORT_DECIMAL));
        assertEquals(getDomain(LONG_DECIMAL, 10, null), Domain.all(LONG_DECIMAL));

        assertEquals(getDomain(SHORT_DECIMAL, 0, decimalColumnStats(null, null, null)), Domain.none(SHORT_DECIMAL));
        assertEquals(getDomain(LONG_DECIMAL, 0, decimalColumnStats(0L, null, null)), Domain.none(LONG_DECIMAL));
        assertEquals(getDomain(SHORT_DECIMAL, 0, decimalColumnStats(0L, "-999.99", "999.99")), Domain.none(SHORT_DECIMAL));

        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(0L, null, null)), onlyNull(LONG_DECIMAL));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, null, null)), notNull(SHORT_DECIMAL));

        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "999.99", "999.99")), singleValue(SHORT_DECIMAL, shortDecimal("999.99")));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "999.9", "999.9")), singleValue(SHORT_DECIMAL, shortDecimal("999.90")));
        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "1234567890.0987654321", "1234567890.0987654321")),
                singleValue(LONG_DECIMAL, longDecimal("1234567890.0987654321")));

        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "-999.99", "999.99")),
                create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("-999.99"), true, shortDecimal("999.99"), true)), false));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "10.5", "20")),
                create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("10.50"), true, shortDecimal("20.00"), true)), false));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, null, "999.99")),
                create(ValueSet.ofRanges(lessThanOrEqual(SHORT_DECIMAL, shortDecimal("999.99"))), false));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "-999.99", null)),
                create(ValueSet.ofRanges(greaterThanOrEqual(SHORT_DECIMAL, shortDecimal("-999.99"))), false));

        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "-1234567890.0987654321", "1234567890.0987654321")),
                create(ValueSet.ofRanges(range(LONG_DECIMAL, longDecimal("-1234567890.0987654321"), true, longDecimal("1234567890.0987654321"), true)), false));
        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, null, "1234567890.0987654321")),
                create(ValueSet.ofRanges(lessThanOrEqual(LONG_DECIMAL, longDecimal("1234567890.0987654321"))), false));
        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "-1234567890.0987654321", null)),
                create(ValueSet.ofRanges(greaterThanOrEqual(LONG_DECIMAL, longDecimal("-1234567890.0987654321"))), false));

        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, "-999.99", "999.99")),
                create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("-999.99"), true, shortDecimal("999.99"), true)), true));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, null, "999.99")),
                create(ValueSet.ofRanges(lessThanOrEqual(SHORT_DECIMAL, shortDecimal("999.99"))), true));
        assertEquals(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, "-999.99", null)),
                create(ValueSet.ofRanges(greaterThanOrEqual(SHORT_DECIMAL, shortDecimal("-999.99"))), true));

        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, "-1234567890.0987654321", "1234567890.0987654321")),
                create(ValueSet.ofRanges(range(LONG_DECIMAL, longDecimal("-1234567890.0987654321"), true, longDecimal("1234567890.0987654321"), true)), true));
        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, null, "1234567890.0987654321")),
                create(ValueSet.ofRanges(lessThanOrEqual(LONG_DECIMAL, longDecimal("1234567890.0987654321"))), true));
        assertEquals(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, "-1234567890.0987654321", null)),
                create(ValueSet.ofRanges(greaterThanOrEqual(LONG_DECIMAL, longDecimal("-1234567890.0987654321"))), true));
    }

    private static ColumnStatistics decimalColumnStats(Long numberOfValues, String minimum, String maximum)
    {
        BigDecimal minimumDecimal = minimum == null ? null : new BigDecimal(minimum);
        BigDecimal maximumDecimal = maximum == null ? null : new BigDecimal(maximum);
        return new DecimalColumnStatistics(numberOfValues, null, new DecimalStatistics(minimumDecimal, maximumDecimal, SHORT_DECIMAL_VALUE_BYTES));
    }

    @Test
    public void testBinary()
    {
        assertEquals(getDomain(VARBINARY, 0, null), Domain.none(VARBINARY));
        assertEquals(getDomain(VARBINARY, 10, null), Domain.all(VARBINARY));

        assertEquals(getDomain(VARBINARY, 0, binaryColumnStats(null)), Domain.none(VARBINARY));
        assertEquals(getDomain(VARBINARY, 0, binaryColumnStats(0L)), Domain.none(VARBINARY));
        assertEquals(getDomain(VARBINARY, 0, binaryColumnStats(0L)), Domain.none(VARBINARY));

        assertEquals(getDomain(VARBINARY, 10, binaryColumnStats(0L)), onlyNull(VARBINARY));
        assertEquals(getDomain(VARBINARY, 10, binaryColumnStats(10L)), notNull(VARBINARY));

        assertEquals(getDomain(VARBINARY, 20, binaryColumnStats(10L)), Domain.all(VARBINARY));
    }

    private static ColumnStatistics binaryColumnStats(Long numberOfValues)
    {
        // sum and minAverageValueSizeInBytes are not used in this test; they could be arbitrary numbers
        return new BinaryColumnStatistics(numberOfValues, null, new BinaryStatistics(100L));
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValue();
    }

    private static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }
}
