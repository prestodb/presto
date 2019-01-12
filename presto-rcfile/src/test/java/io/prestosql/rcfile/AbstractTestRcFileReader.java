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
package com.facebook.presto.rcfile;

import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.rcfile.RcFileTester.Format.BINARY;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestRcFileReader
{
    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);

    private final RcFileTester tester;

    public AbstractTestRcFileReader(RcFileTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), RcFileTester.HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testNoData()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, ImmutableList.of());
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(BOOLEAN, limit(cycle(ImmutableList.of(true, false, false)), 3_000));
    }

    @Test
    public void testByteSequence()
            throws Exception
    {
        tester.testRoundTrip(
                TINYINT,
                intsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 11 == 0)
                        .map(Integer::byteValue) // truncate values to byte range
                        .collect(toList()));
    }

    @Test
    public void testShortSequence()
            throws Exception
    {
        tester.testRoundTrip(
                SMALLINT,
                intsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 11 == 0)
                        .map(Integer::shortValue) // truncate values to short range
                        .collect(toList()));
        tester.testRoundTrip(
                SMALLINT,
                intsBetween(Short.MIN_VALUE, Short.MIN_VALUE + 1000).stream()
                        .map(Integer::shortValue) // truncate values to short range
                        .collect(toList()));
        tester.testRoundTrip(
                SMALLINT,
                intsBetween(Short.MAX_VALUE - 1000, Short.MAX_VALUE).stream()
                        .map(Integer::shortValue) // truncate values to short range
                        .collect(toList()));
    }

    @Test
    public void testIntSequence()
            throws Exception
    {
        tester.testRoundTrip(
                INTEGER,
                intsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 11 == 0)
                        .collect(toList()));
        tester.testRoundTrip(INTEGER, intsBetween(Integer.MIN_VALUE, Integer.MIN_VALUE + 1000));
        tester.testRoundTrip(INTEGER, intsBetween(Integer.MAX_VALUE - 1000, Integer.MAX_VALUE));
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        tester.testRoundTrip(
                BIGINT,
                longsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 11 == 0)
                        .collect(toList()));
        tester.testRoundTrip(BIGINT, longsBetween(Long.MIN_VALUE, Long.MIN_VALUE + 1000));
        tester.testRoundTrip(BIGINT, longsBetween(Long.MAX_VALUE - 1000, Long.MAX_VALUE));
    }

    @Test
    public void testDateSequence()
            throws Exception
    {
        tester.testRoundTrip(
                DATE,
                intsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 11 == 0)
                        .map(SqlDate::new)
                        .collect(toList()));
    }

    @Test
    public void testTimestampSequence()
            throws Exception
    {
        tester.testRoundTrip(
                TIMESTAMP,
                intsBetween(-31_234, 31_234).stream()
                        .filter(i -> i % 19 == 0)
                        .map(timestamp -> sqlTimestampOf(timestamp, SESSION))
                        .collect(toList()));
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        // this results in rounding errors inside of structural types
        tester.testRoundTrip(
                REAL,
                doubleSequence(-500.12f, 1.0f, 3_000).stream()
                        .map(Double::floatValue)
                        .collect(toList()));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(-500.12, 1, 3_000));
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "6", 1_000, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_8, decimalSequence("-3000000", "6000", 1_000, 8, 4));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_17, decimalSequence("-30000000000", "60000000", 1_000, 17, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, decimalSequence("-30000000000", "60000000", 1_000, 18, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, decimalSequence("-3000000000000000000", "6000000000000000", 1_000, 38, 16));
    }

    @Test
    public void testStringSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                intsBetween(0, 31_234).stream()
                        .filter(i -> i % 19 == 0)
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, nCopies(3_000, ""));
    }

    @Test
    public void testBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY,
                intsBetween(0, 30_000).stream()
                        .filter(i -> i % 19 == 0)
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        // Binary serde can not serialize an empty binary sequence
        tester.testRoundTrip(VARBINARY, nCopies(3_000, new SqlVarbinary(new byte[0])), BINARY);
    }

    private static List<Double> doubleSequence(double start, double step, int items)
    {
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static ContiguousSet<Long> longsBetween(long lowerInclusive, long upperExclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerInclusive, upperExclusive), DiscreteDomain.longs());
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static List<SqlDecimal> decimalSequence(String start, String step, int items, int precision, int scale)
    {
        BigInteger decimalStep = new BigInteger(step);

        List<SqlDecimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);
        for (int i = 0; i < items; i++) {
            values.add(new SqlDecimal(nextValue, precision, scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }
}
