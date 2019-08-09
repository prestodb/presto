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

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.quickSelectiveOrcTester;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestSelectiveOrcReader
{
    private final OrcTester tester = quickSelectiveOrcTester();

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, BooleanValue.of(true, false)),
                ImmutableMap.of(0, TupleDomainFilter.IS_NULL));
        tester.testRoundTrip(BOOLEAN, newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)), filters);

        filters = ImmutableList.of(
                ImmutableMap.of(0, BooleanValue.of(true, false)),
                ImmutableMap.of(0, TupleDomainFilter.IS_NULL),
                ImmutableMap.of(1, BooleanValue.of(true, false)),
                ImmutableMap.of(0, BooleanValue.of(false, false), 1, BooleanValue.of(true, false)));
        tester.testRoundTripTypes(ImmutableList.of(BOOLEAN, BOOLEAN),
                ImmutableList.of(
                        newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)),
                        newArrayList(limit(cycle(ImmutableList.of(true, true, false)), 30_000))),
                filters);
    }

    @Test
    public void testByteValues()
            throws Exception
    {
        List<Byte> byteValues = ImmutableList.of(1, 3, 5, 7, 11, 13, 17)
                .stream()
                .map(Integer::byteValue)
                .collect(toList());

        tester.testRoundTrip(TINYINT, byteValues,
                ImmutableList.of(
                        ImmutableMap.of(0, BigintValues.of(new long[] {1, 17}, false)),
                        ImmutableMap.of(0, IS_NULL)));

        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, BigintRange.of(1, 17, false)),
                ImmutableMap.of(0, IS_NULL),
                ImmutableMap.of(1, IS_NULL),
                ImmutableMap.of(
                        0,
                        BigintRange.of(7, 17, false),
                        1,
                        BigintRange.of(12, 14, false)));
        tester.testRoundTripTypes(ImmutableList.of(TINYINT, TINYINT), ImmutableList.of(byteValues, byteValues), filters);
    }

    @Test
    public void testByteValuesRepeat()
            throws Exception
    {
        List<Byte> byteValues = ImmutableList.of(1, 3, 5, 7, 11, 13, 17)
                .stream()
                .map(Integer::byteValue)
                .collect(toList());
        tester.testRoundTrip(
                TINYINT,
                newArrayList(limit(repeatEach(4, cycle(byteValues)), 30_000)),
                ImmutableList.of(ImmutableMap.of(0, BigintRange.of(1, 14, true))));
    }

    @Test
    public void testByteValuesPatchedBase()
            throws Exception
    {
        List<Byte> byteValues = newArrayList(
                limit(cycle(concat(
                        intsBetween(0, 18),
                        intsBetween(0, 18),
                        ImmutableList.of(30_000, 20_000, 400_000, 30_000, 20_000))), 30_000))
                .stream()
                .map(Integer::byteValue)
                .collect(toList());
        tester.testRoundTrip(
                TINYINT,
                byteValues,
                ImmutableList.of(ImmutableMap.of(0, BigintRange.of(4, 14, true))));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, DoubleRange.of(0, false, false, 1_000, false, false, false)),
                ImmutableMap.of(0, IS_NULL),
                ImmutableMap.of(0, IS_NOT_NULL));

        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000), filters);
        tester.testRoundTripTypes(
                ImmutableList.of(DOUBLE, DOUBLE),
                ImmutableList.of(
                        doubleSequence(0, 0.1, 10_000),
                        doubleSequence(0, 0.1, 10_000)),
                ImmutableList.of(
                        ImmutableMap.of(
                                0, DoubleRange.of(1.0, false, true, 7.0, false, true, true),
                                1, DoubleRange.of(3.0, false, true, 9.0, false, true, false))));
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, DoubleRange.of(0, false, false, 1_000, false, false, false)),
                ImmutableMap.of(0, IS_NULL),
                ImmutableMap.of(0, IS_NOT_NULL));

        tester.testRoundTrip(DOUBLE, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), filters);

        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), filters);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234), BigintRange.of(10, 100, false));
    }

    @Test
    public void testNegativeLongSequence()
            throws Exception
    {
        // A flaw in ORC encoding makes it impossible to represent timestamp
        // between 1969-12-31 23:59:59.000, exclusive, and 1970-01-01 00:00:00.000, exclusive.
        // Therefore, such data won't round trip and are skipped from test.
        testRoundTripNumeric(intsBetween(-31_234, -999), BigintRange.of(-1000, -100, true));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)), BigintRange.of(10, 100, false));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000), BigintRange.of(4, 14, false));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = IntStream.range(0, 31_234).boxed().collect(toList());
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values, BigintRange.of(4, 14, false));
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000), BigintRange.of(4, 14, true));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), intsBetween(0, 18), ImmutableList.of(30_000, 20_000, 400_000, 30_000, 20_000))), 30_000),
                BigintValues.of(new long[] {0, 5, 10, 15, 20_000}, true));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)), BigintRange.of(123, 123, true));
    }

    @Test
    public void testFloats()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, FloatRange.of(0.0f, false, true, 100.0f, false, true, true)),
                ImmutableMap.of(0, FloatRange.of(-100.0f, false, true, 0.0f, false, true, false)),
                ImmutableMap.of(0, IS_NULL));

        tester.testRoundTrip(REAL, ImmutableList.copyOf(repeatEach(10, ImmutableList.of(-100.0f, 0.0f, 100.0f))), filters);
        tester.testRoundTrip(REAL, ImmutableList.copyOf(repeatEach(10, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY))));

        List<Float> floatValues = ImmutableList.of(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f);
        tester.testRoundTripTypes(
                ImmutableList.of(REAL, REAL),
                ImmutableList.of(
                        ImmutableList.copyOf(limit(repeatEach(4, cycle(floatValues)), 100)),
                        ImmutableList.copyOf(limit(repeatEach(4, cycle(floatValues)), 100))),
                ImmutableList.of(
                        ImmutableMap.of(
                                0, FloatRange.of(1.0f, false, true, 7.0f, false, true, true),
                                1, FloatRange.of(3.0f, false, true, 9.0f, false, true, false)),
                        ImmutableMap.of(
                                1, FloatRange.of(1.0f, false, true, 7.0f, false, true, true))));
    }

    @Test
    public void testArrays()
            throws Exception
    {
        Random random = new Random(0);

        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(INTEGER)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).boxed().map(i -> random.nextInt()).collect(toImmutableList()),
                        IntStream.range(0, 30_000).boxed().map(i -> IntStream.range(0, random.nextInt(10)).map(j -> j + i).boxed().collect(toImmutableList())).collect(toImmutableList())),
                ImmutableList.of(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(1, IS_NOT_NULL)));

        tester.testRoundTripTypes(ImmutableList.of(arrayType(INTEGER), INTEGER),
                ImmutableList.of(
                        IntStream.range(0, 30_000).boxed().map(i -> i % 7 == 0 ? null : IntStream.range(0, random.nextInt(10)).map(j -> j + i).boxed().collect(toImmutableList())).collect(toList()),
                        IntStream.range(0, 30_000).boxed().map(i -> i % 11 == 0 ? null : random.nextInt()).collect(toList())),
                ImmutableList.of(
                        ImmutableMap.of(0, IS_NOT_NULL, 1, IS_NULL)));
    }

    @Test
    public void testStructs()
            throws Exception
    {
        Random random = new Random(0);

        tester.testRoundTripTypes(ImmutableList.of(INTEGER, rowType(INTEGER, BOOLEAN)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).boxed().map(i -> random.nextInt()).collect(toImmutableList()),
                        IntStream.range(0, 30_000).boxed().map(i -> ImmutableList.of(random.nextInt(), random.nextBoolean())).collect(toImmutableList())),
                ImmutableList.of(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(1, IS_NOT_NULL)));

        tester.testRoundTripTypes(ImmutableList.of(rowType(INTEGER, BOOLEAN), INTEGER),
                ImmutableList.of(
                        IntStream.range(0, 30_000).boxed().map(i -> i % 7 == 0 ? null : ImmutableList.of(random.nextInt(), random.nextBoolean())).collect(toList()),
                        IntStream.range(0, 30_000).boxed().map(i -> i % 11 == 0 ? null : random.nextInt()).collect(toList())),
                ImmutableList.of(
                        ImmutableMap.of(0, IS_NOT_NULL, 1, IS_NULL)));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values, TupleDomainFilter filter)
            throws Exception
    {
        List<Long> longValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());

        List<Integer> intValues = longValues.stream()
                .map(Long::intValue) // truncate values to int range
                .collect(toList());

        List<Short> shortValues = longValues.stream()
                .map(Long::shortValue) // truncate values to short range
                .collect(toList());

        List<SqlDate> dateValues = longValues.stream()
                .map(Long::intValue)
                .map(SqlDate::new)
                .collect(toList());

        List<SqlTimestamp> timestamps = longValues.stream()
                .map(timestamp -> sqlTimestampOf(timestamp, SESSION))
                .collect(toList());

        tester.testRoundTrip(BIGINT, longValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(INTEGER, intValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(SMALLINT, shortValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(DATE, dateValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(TIMESTAMP, timestamps, ImmutableList.of(ImmutableMap.of(0, filter)));

        List<Integer> reversedIntValues = new ArrayList<>(intValues);
        Collections.reverse(reversedIntValues);

        List<SqlDate> reversedDateValues = new ArrayList<>(dateValues);
        Collections.reverse(reversedDateValues);

        List<SqlTimestamp> reversedTimestampValues = new ArrayList<>(timestamps);
        Collections.reverse(reversedTimestampValues);

        tester.testRoundTripTypes(ImmutableList.of(BIGINT, INTEGER, SMALLINT, DATE, TIMESTAMP),
                ImmutableList.of(
                        longValues,
                        reversedIntValues,
                        shortValues,
                        reversedDateValues,
                        reversedTimestampValues),
                ImmutableList.of(
                        ImmutableMap.of(0, filter),
                        ImmutableMap.of(1, filter),
                        ImmutableMap.of(0, filter, 1, filter),
                        ImmutableMap.of(0, filter, 1, filter, 2, filter)));
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    private static List<Double> doubleSequence(double start, double step, int items)
    {
        return IntStream.range(0, items)
                .mapToDouble(i -> start + i * step)
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }
}
