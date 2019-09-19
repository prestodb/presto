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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.mapType;
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
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSelectiveOrcReader
{
    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_19 = DecimalType.createDecimalType(19, 8);
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
        tester.testRoundTrip(
                BOOLEAN,
                newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)),
                BooleanValue.of(true, false), TupleDomainFilter.IS_NULL);

        tester.testRoundTripTypes(ImmutableList.of(BOOLEAN, BOOLEAN),
                ImmutableList.of(
                        newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)),
                        newArrayList(limit(cycle(ImmutableList.of(true, true, false)), 30_000))),
                toSubfieldFilters(
                        ImmutableMap.of(0, BooleanValue.of(true, false)),
                        ImmutableMap.of(0, TupleDomainFilter.IS_NULL),
                        ImmutableMap.of(1, BooleanValue.of(true, false)),
                        ImmutableMap.of(0, BooleanValue.of(false, false), 1, BooleanValue.of(true, false))));
    }

    @Test
    public void testByteValues()
            throws Exception
    {
        List<Byte> byteValues = ImmutableList.of(1, 3, 5, 7, 11, 13, 17)
                .stream()
                .map(Integer::byteValue)
                .collect(toList());

        tester.testRoundTrip(TINYINT, byteValues, BigintValues.of(new long[] {1, 17}, false), IS_NULL);

        List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters = toSubfieldFilters(
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
                BigintRange.of(1, 14, true));
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
                BigintRange.of(4, 14, true));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000), DoubleRange.of(0, false, false, 1_000, false, false, false), IS_NULL, IS_NOT_NULL);
        tester.testRoundTripTypes(
                ImmutableList.of(DOUBLE, DOUBLE),
                ImmutableList.of(
                        doubleSequence(0, 0.1, 10_000),
                        doubleSequence(0, 0.1, 10_000)),
                toSubfieldFilters(
                        ImmutableMap.of(
                                0, DoubleRange.of(1.0, false, true, 7.0, false, true, true),
                                1, DoubleRange.of(3.0, false, true, 9.0, false, true, false))));
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        List<Map<Subfield, TupleDomainFilter>> filters = toSubfieldFilters(
                DoubleRange.of(0, false, false, 1_000, false, false, false),
                IS_NULL,
                IS_NOT_NULL);

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
        List<Map<Subfield, TupleDomainFilter>> filters = toSubfieldFilters(
                FloatRange.of(0.0f, false, true, 100.0f, false, true, true),
                FloatRange.of(-100.0f, false, true, 0.0f, false, true, false),
                IS_NULL);

        tester.testRoundTrip(REAL, ImmutableList.copyOf(repeatEach(10, ImmutableList.of(-100.0f, 0.0f, 100.0f))), filters);
        tester.testRoundTrip(REAL, ImmutableList.copyOf(repeatEach(10, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY))));

        List<Float> floatValues = ImmutableList.of(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f);
        tester.testRoundTripTypes(
                ImmutableList.of(REAL, REAL),
                ImmutableList.of(
                        ImmutableList.copyOf(limit(repeatEach(4, cycle(floatValues)), 100)),
                        ImmutableList.copyOf(limit(repeatEach(4, cycle(floatValues)), 100))),
                toSubfieldFilters(
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

        // non-null arrays of varying sizes; some arrays may be empty
        tester.testRoundTrip(arrayType(INTEGER),
                IntStream.range(0, 30_000).map(i -> random.nextInt(10)).mapToObj(size -> makeArray(size, random)).collect(toImmutableList()),
                IS_NULL, IS_NOT_NULL);

        BigintRange negative = BigintRange.of(Integer.MIN_VALUE, 0, false);
        BigintRange nonNegative = BigintRange.of(0, Integer.MAX_VALUE, false);

        // non-empty non-null arrays of varying sizes
        tester.testRoundTrip(arrayType(INTEGER),
                IntStream.range(0, 30_000).map(i -> 5 + random.nextInt(5)).mapToObj(size -> makeArray(size, random)).collect(toImmutableList()),
                ImmutableList.of(
                        toSubfieldFilter(IS_NULL),
                        toSubfieldFilter(IS_NOT_NULL),
                        // c[1] >= 0
                        toSubfieldFilter("c[1]", nonNegative),
                        // c[2] >= 0 AND c[4] >= 0
                        ImmutableMap.of(
                                new Subfield("c[2]"), nonNegative,
                                new Subfield("c[4]"), nonNegative)));

        // non-null arrays of varying sizes; some arrays may be empty
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(INTEGER)),
                ImmutableList.of(
                        makeArray(30_000, random),
                        IntStream.range(0, 30_000).map(i -> random.nextInt(10)).mapToObj(size -> makeArray(size, random)).collect(toImmutableList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, nonNegative),
                        ImmutableMap.of(
                                0, nonNegative,
                                1, IS_NULL),
                        ImmutableMap.of(
                                0, nonNegative,
                                1, IS_NOT_NULL)));

        // non-empty non-null arrays of varying sizes
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(INTEGER)),
                ImmutableList.of(
                        makeArray(30_000, random),
                        IntStream.range(0, 30_000).map(i -> 5 + random.nextInt(5)).mapToObj(size -> makeArray(size, random)).collect(toImmutableList())),
                ImmutableList.of(
                        // c[1] >= 0
                        ImmutableMap.of(
                                0, toSubfieldFilter(nonNegative),
                                1, toSubfieldFilter("c[1]", nonNegative)),
                        // c[3] >= 0
                        ImmutableMap.of(
                                0, toSubfieldFilter(nonNegative),
                                1, toSubfieldFilter("c[3]", nonNegative)),
                        // c[2] >= 0 AND c[4] <= 0
                        ImmutableMap.of(
                                0, toSubfieldFilter(nonNegative),
                                1, ImmutableMap.of(
                                        new Subfield("c[2]"), nonNegative,
                                        new Subfield("c[4]"), negative))));

        // nested arrays
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(arrayType(INTEGER))),
                ImmutableList.of(
                        makeArray(30_000, random),
                        IntStream.range(0, 30_000).map(i -> random.nextInt(10)).mapToObj(size -> IntStream.range(0, size).map(i -> random.nextInt(5)).mapToObj(nestedSize -> makeArray(nestedSize, random)).collect(toImmutableList())).collect(toImmutableList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, nonNegative),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(1, IS_NOT_NULL),
                        ImmutableMap.of(
                                0, nonNegative,
                                1, IS_NULL)));

        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(arrayType(INTEGER))),
                ImmutableList.of(
                        makeArray(30_000, random),
                        IntStream.range(0, 30_000).map(i -> 3 + random.nextInt(10)).mapToObj(size -> IntStream.range(0, size).map(i -> 3 + random.nextInt(5)).mapToObj(nestedSize -> makeArray(nestedSize, random)).collect(toImmutableList())).collect(toImmutableList())),
                ImmutableList.of(
                        // c[1] IS NULL
                        ImmutableMap.of(1, ImmutableMap.of(new Subfield("c[1]"), IS_NULL)),
                        // c[2] IS NOT NULL AND c[2][3] >= 0
                        ImmutableMap.of(1, ImmutableMap.of(
                                new Subfield("c[2]"), IS_NOT_NULL,
                                new Subfield("c[2][3]"), nonNegative)),
                        ImmutableMap.of(
                                0, toSubfieldFilter(nonNegative),
                                1, ImmutableMap.of(new Subfield("c[1]"), IS_NULL))));
    }

    @Test
    public void testArrayIndexOutOfBounds()
            throws Exception
    {
        Random random = new Random(0);

        // non-null arrays of varying sizes
        try {
            tester.testRoundTrip(arrayType(INTEGER),
                    IntStream.range(0, 30_000).map(i -> random.nextInt(10)).mapToObj(size -> makeArray(size, random)).collect(toImmutableList()),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // non-null nested arrays of varying sizes
        try {
            tester.testRoundTrip(arrayType(arrayType(INTEGER)),
                    IntStream.range(0, 30_000).mapToObj(i -> ImmutableList.of(makeArray(random.nextInt(5), random), makeArray(random.nextInt(5), random))).collect(toImmutableList()),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2][3]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // empty arrays
        try {
            tester.testRoundTrip(arrayType(INTEGER),
                    nCopies(30_000, ImmutableList.of()),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // empty nested arrays
        try {
            tester.testRoundTrip(arrayType(arrayType(INTEGER)),
                    nCopies(30_000, ImmutableList.of()),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2][3]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }
    }

    @Test
    public void testArraysOfNulls()
            throws Exception
    {
        for (Type type : ImmutableList.of(BOOLEAN, BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, TIMESTAMP, DECIMAL_TYPE_PRECISION_19, DECIMAL_TYPE_PRECISION_4, arrayType(INTEGER))) {
            tester.testRoundTrip(arrayType(type),
                    nCopies(30_000, nCopies(5, null)),
                    ImmutableList.of(
                            ImmutableMap.of(new Subfield("c[2]"), IS_NULL),
                            ImmutableMap.of(new Subfield("c[2]"), IS_NOT_NULL)));
        }
    }

    @Test
    public void testStructs()
            throws Exception
    {
        Random random = new Random(0);

        tester.testRoundTripTypes(ImmutableList.of(INTEGER, rowType(INTEGER, BOOLEAN)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> random.nextInt()).collect(toImmutableList()),
                        IntStream.range(0, 30_000).mapToObj(i -> ImmutableList.of(random.nextInt(), random.nextBoolean())).collect(toImmutableList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(1, IS_NOT_NULL)));

        tester.testRoundTripTypes(ImmutableList.of(rowType(INTEGER, BOOLEAN), INTEGER),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> i % 7 == 0 ? null : ImmutableList.of(random.nextInt(), random.nextBoolean())).collect(toList()),
                        IntStream.range(0, 30_000).mapToObj(i -> i % 11 == 0 ? null : random.nextInt()).collect(toList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, IS_NOT_NULL, 1, IS_NULL)));
    }

    @Test
    public void testMaps()
            throws Exception
    {
        Random random = new Random(0);

        // map column with no nulls
        tester.testRoundTripTypes(
                ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> random.nextInt()).collect(toImmutableList()),
                        IntStream.range(0, 30_000).mapToObj(i -> createMap(i)).collect(toImmutableList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NOT_NULL),
                        ImmutableMap.of(1, IS_NULL)));

        // map column with nulls
        tester.testRoundTripTypes(
                ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> random.nextInt()).collect(toImmutableList()),
                        IntStream.range(0, 30_000).mapToObj(i -> i % 5 == 0 ? null : createMap(i)).collect(toList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NOT_NULL),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false), 1, IS_NULL),
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false), 1, IS_NOT_NULL)));

        // map column with filter, followed by another column with filter
        tester.testRoundTripTypes(
                ImmutableList.of(mapType(INTEGER, INTEGER), INTEGER),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> i % 5 == 0 ? null : createMap(i)).collect(toList()),
                        IntStream.range(0, 30_000).mapToObj(i -> random.nextInt()).collect(toImmutableList())),
                toSubfieldFilters(
                        ImmutableMap.of(0, IS_NULL, 1, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(0, IS_NOT_NULL, 1, BigintRange.of(0, Integer.MAX_VALUE, false))));

        // empty maps
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        IntStream.range(0, 30_000).mapToObj(i -> random.nextInt()).collect(toImmutableList()),
                        Collections.nCopies(30_000, ImmutableMap.of())),
                ImmutableList.of());
    }

    private static Map<Integer, Integer> createMap(int seed)
    {
        int mapSize = Math.abs(seed) % 7 + 1;
        return IntStream.range(0, mapSize).boxed().collect(toImmutableMap(Function.identity(), i -> i + seed));
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_19, decimalSequence("-3000000000000000000", "100000000000000101", 60, 19, 8));

        tester.testRoundTripTypes(
                ImmutableList.of(DECIMAL_TYPE_PRECISION_2, DECIMAL_TYPE_PRECISION_2),
                ImmutableList.of(
                        decimalSequence("-30", "1", 60, 2, 1),
                        decimalSequence("-30", "1", 60, 2, 1)),
                toSubfieldFilters(ImmutableMap.of(0, TupleDomainFilter.BigintRange.of(10, 20, true))));

        tester.testRoundTripTypes(
                ImmutableList.of(DECIMAL_TYPE_PRECISION_2, DECIMAL_TYPE_PRECISION_2),
                ImmutableList.of(
                        decimalSequence("-30", "1", 60, 2, 1),
                        decimalSequence("-30", "1", 60, 2, 1)),
                toSubfieldFilters(ImmutableMap.of(
                        0, TupleDomainFilter.BigintRange.of(10, 30, true),
                        1, TupleDomainFilter.BigintRange.of(15, 25, true))));

        tester.testRoundTripTypes(
                ImmutableList.of(DECIMAL_TYPE_PRECISION_19, DECIMAL_TYPE_PRECISION_19),
                ImmutableList.of(
                        decimalSequence("-3000000000000000000", "100000000000000101", 60, 19, 8),
                        decimalSequence("-3000000000000000000", "100000000000000101", 60, 19, 8)),
                toSubfieldFilters(ImmutableMap.of(
                        0, TupleDomainFilter.LongDecimalRange.of(-28999999999L, -28999999999L,
                                false, true, 28999999999L, 28999999999L, false, true, true),
                        1, TupleDomainFilter.LongDecimalRange.of(1000000000L, 1000000000L,
                                false, true, 28999999999L, 28999999999L, false, true, true))));
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

        tester.testRoundTrip(BIGINT, longValues, toSubfieldFilters(filter));

        tester.testRoundTrip(INTEGER, intValues, toSubfieldFilters(filter));

        tester.testRoundTrip(SMALLINT, shortValues, toSubfieldFilters(filter));

        tester.testRoundTrip(DATE, dateValues, toSubfieldFilters(filter));

        tester.testRoundTrip(TIMESTAMP, timestamps, toSubfieldFilters(filter));

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
                toSubfieldFilters(
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

    private static Map<Subfield, TupleDomainFilter> toSubfieldFilter(String subfield, TupleDomainFilter filter)
    {
        return ImmutableMap.of(new Subfield(subfield), filter);
    }

    private static Map<Subfield, TupleDomainFilter> toSubfieldFilter(TupleDomainFilter filter)
    {
        return ImmutableMap.of(new Subfield("c"), filter);
    }

    private static List<Map<Subfield, TupleDomainFilter>> toSubfieldFilters(TupleDomainFilter... filters)
    {
        return Arrays.stream(filters).map(TestSelectiveOrcReader::toSubfieldFilter).collect(toImmutableList());
    }

    private static List<Map<Integer, Map<Subfield, TupleDomainFilter>>> toSubfieldFilters(Map<Integer, TupleDomainFilter>... filters)
    {
        return Arrays.stream(filters)
                .map(columnFilters -> Maps.transformValues(columnFilters, TestSelectiveOrcReader::toSubfieldFilter))
                .collect(toImmutableList());
    }

    private static List<Integer> makeArray(int size, Random random)
    {
        return IntStream.range(0, size).map(i -> random.nextInt()).boxed().collect(toImmutableList());
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
