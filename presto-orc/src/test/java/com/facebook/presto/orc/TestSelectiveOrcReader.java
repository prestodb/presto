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

import com.facebook.presto.orc.OrcTester.OrcReaderSettings;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
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
import static com.facebook.presto.spi.type.CharType.createCharType;
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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSelectiveOrcReader
{
    private static final int NUM_ROWS = 31_234;

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_19 = DecimalType.createDecimalType(19, 8);
    private static final CharType CHAR_10 = createCharType(10);
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
                newArrayList(limit(cycle(ImmutableList.of(true, false, false)), NUM_ROWS)),
                BooleanValue.of(true, false), TupleDomainFilter.IS_NULL);

        tester.testRoundTripTypes(ImmutableList.of(BOOLEAN, BOOLEAN),
                ImmutableList.of(
                        newArrayList(limit(cycle(ImmutableList.of(true, false, false)), NUM_ROWS)),
                        newArrayList(limit(cycle(ImmutableList.of(true, true, false)), NUM_ROWS))),
                toSubfieldFilters(
                        ImmutableMap.of(0, BooleanValue.of(true, false)),
                        ImmutableMap.of(0, TupleDomainFilter.IS_NULL),
                        ImmutableMap.of(1, BooleanValue.of(true, false)),
                        ImmutableMap.of(0, BooleanValue.of(false, false), 1, BooleanValue.of(true, false))));

        tester.testRoundTripTypes(
                ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN),
                ImmutableList.of(newArrayList(true, false, null, false, true), newArrayList(null, null, null, null, null), newArrayList(true, false, null, false, null)),
                toSubfieldFilters(ImmutableMap.of(0, BooleanValue.of(true, false), 1, BooleanValue.of(true, true), 2, BooleanValue.of(true, true))));
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

        tester.testRoundTripTypes(
                ImmutableList.of(TINYINT, TINYINT, TINYINT),
                ImmutableList.of(toByteArray(newArrayList(1, 2, null, 3, 4)), newArrayList(null, null, null, null, null), toByteArray(newArrayList(5, 6, null, 7, null))),
                toSubfieldFilters(ImmutableMap.of(
                        0, BigintValues.of(new long[] {1, 4}, false),
                        1, BigintValues.of(new long[] {1, 5}, true),
                        2, BigintValues.of(new long[] {5, 7}, true))));
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
                newArrayList(limit(repeatEach(4, cycle(byteValues)), NUM_ROWS)),
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
                        ImmutableList.of(NUM_ROWS, 20_000, 400_000, NUM_ROWS, 20_000))), NUM_ROWS))
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
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, NUM_ROWS), DoubleRange.of(0, false, false, 1_000, false, false, false), IS_NULL, IS_NOT_NULL);
        tester.testRoundTripTypes(
                ImmutableList.of(DOUBLE, DOUBLE),
                ImmutableList.of(
                        doubleSequence(0, 0.1, 10_000),
                        doubleSequence(0, 0.1, 10_000)),
                toSubfieldFilters(
                        ImmutableMap.of(
                                0, DoubleRange.of(1.0, false, true, 7.0, false, true, true),
                                1, DoubleRange.of(3.0, false, true, 9.0, false, true, false))));
        tester.testRoundTripTypes(
                ImmutableList.of(DOUBLE, DOUBLE, DOUBLE),
                ImmutableList.of(newArrayList(1.0, 2.0, null, 3.0, 4.0), newArrayList(null, null, null, null, null), newArrayList(1.0, 2.0, null, 3.0, null)),
                toSubfieldFilters(ImmutableMap.of(
                        0, DoubleRange.of(1.0, false, true, 7.0, false, true, true),
                        1, DoubleRange.of(1.0, false, true, 7.0, false, true, true),
                        2, DoubleRange.of(1.0, false, true, 7.0, false, true, true))));
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
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), NUM_ROWS), BigintRange.of(4, 14, false));

        Random random = new Random(0);

        // read selected positions from all nulls column
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, INTEGER),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> random.nextInt(10)),
                        createList(NUM_ROWS, i -> null)),
                toSubfieldFilters(ImmutableMap.of(0, BigintRange.of(0, 5, false))));
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
    public void testLongDirectVarintScale()
            throws Exception
    {
        List<Long> values = varintScaleSequence(NUM_ROWS);
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values, BigintRange.of(0, 1L << 60, false));
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), NUM_ROWS), BigintRange.of(4, 14, true));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), intsBetween(0, 18), ImmutableList.of(NUM_ROWS, 20_000, 400_000, NUM_ROWS, 20_000))), NUM_ROWS),
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

        tester.testRoundTripTypes(
                ImmutableList.of(REAL, REAL, REAL),
                ImmutableList.of(newArrayList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f), newArrayList(null, null, null, null, null), newArrayList(1.0f, 2.0f, null, 3.0f, null)),
                toSubfieldFilters(ImmutableMap.of(
                        0, FloatRange.of(2.0f, false, false, 7.0f, false, true, true),
                        1, FloatRange.of(1.0f, false, false, 7.0f, false, true, true),
                        2, FloatRange.of(1.0f, false, false, 7.0f, false, true, true))));
    }

    @Test
    public void testFilterOrder()
            throws Exception
    {
        Random random = new Random(0);

        tester.testRoundTripTypesWithOrder(ImmutableList.of(INTEGER, INTEGER),
                ImmutableList.of(newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11)), NUM_ROWS)), randomIntegers(NUM_ROWS, random)),
                toSubfieldFilters(
                        ImmutableMap.of(
                                0, BigintRange.of(1, 1, true),
                                1, BigintRange.of(2, 200, true))),
                ImmutableList.of(ImmutableList.of(0, 1)));

        tester.testRoundTripTypesWithOrder(ImmutableList.of(INTEGER, INTEGER),
                ImmutableList.of(newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11)), NUM_ROWS)), randomIntegers(NUM_ROWS, random)),
                toSubfieldFilters(
                        ImmutableMap.of(
                                0, BigintRange.of(100, 100, false),
                                1, BigintRange.of(2, 200, true))),
                ImmutableList.of(ImmutableList.of(0)));

        tester.testRoundTripTypesWithOrder(
                ImmutableList.of(INTEGER, INTEGER, DOUBLE, arrayType(INTEGER)),
                ImmutableList.of(newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11)), NUM_ROWS)), createList(NUM_ROWS, i -> random.nextInt(200)), doubleSequence(0, 0.1, NUM_ROWS), nCopies(NUM_ROWS, randomIntegers(5, random))),
                ImmutableList.of(
                        ImmutableMap.of(
                                0, toSubfieldFilter(BigintRange.of(1, 1, true)),
                                1, toSubfieldFilter(BigintRange.of(0, 200, true)),
                                2, toSubfieldFilter(DoubleRange.of(0, false, false, 0.1, false, false, true)),
                                3, toSubfieldFilter("c[1]", BigintRange.of(4, 4, false)))),
                ImmutableList.of(ImmutableList.of(0, 1, 2, 3)));
    }

    @Test
    public void testArrays()
            throws Exception
    {
        Random random = new Random(0);

        // non-null arrays of varying sizes; some arrays may be empty
        tester.testRoundTrip(arrayType(INTEGER),
                createList(NUM_ROWS, i -> randomIntegers(random.nextInt(10), random)),
                IS_NULL, IS_NOT_NULL);

        BigintRange negative = BigintRange.of(Integer.MIN_VALUE, 0, false);
        BigintRange nonNegative = BigintRange.of(0, Integer.MAX_VALUE, false);

        // non-empty non-null arrays of varying sizes
        tester.testRoundTrip(arrayType(INTEGER),
                createList(NUM_ROWS, i -> randomIntegers(5 + random.nextInt(5), random)),
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
                        randomIntegers(NUM_ROWS, random),
                        createList(NUM_ROWS, i -> randomIntegers(random.nextInt(10), random))),
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
                        randomIntegers(NUM_ROWS, random),
                        createList(NUM_ROWS, i -> randomIntegers(5 + random.nextInt(5), random))),
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
                        randomIntegers(NUM_ROWS, random),
                        createList(NUM_ROWS, i -> createList(random.nextInt(10), index -> randomIntegers(random.nextInt(5), random)))),
                toSubfieldFilters(
                        ImmutableMap.of(0, nonNegative),
                        ImmutableMap.of(1, IS_NULL),
                        ImmutableMap.of(1, IS_NOT_NULL),
                        ImmutableMap.of(
                                0, nonNegative,
                                1, IS_NULL)));

        tester.testRoundTripTypes(ImmutableList.of(INTEGER, arrayType(arrayType(INTEGER))),
                ImmutableList.of(
                        randomIntegers(NUM_ROWS, random),
                        createList(NUM_ROWS, i -> createList(3 + random.nextInt(10), index -> randomIntegers(3 + random.nextInt(5), random)))),
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
    public void testArraysWithSubfieldPruning()
            throws Exception
    {
        tester.assertRoundTripWithSettings(arrayType(INTEGER),
                createList(NUM_ROWS, i -> ImmutableList.of(1, 2, 3, 4)),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1]", "c[2]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2]").build()));

        Random random = new Random(0);

        tester.assertRoundTripWithSettings(arrayType(INTEGER),
                createList(NUM_ROWS, i -> ImmutableList.of(random.nextInt(10), random.nextInt(10), 3, 4)),
                ImmutableList.of(
                        OrcReaderSettings.builder()
                                .addRequiredSubfields(0, "c[1]", "c[3]")
                                .setColumnFilters(ImmutableMap.of(0, ImmutableMap.of(new Subfield("c[1]"), BigintRange.of(0, 4, false))))
                                .build(),
                        OrcReaderSettings.builder()
                                .addRequiredSubfields(0, "c[2]", "c[3]")
                                .setColumnFilters(ImmutableMap.of(0, ImmutableMap.of(new Subfield("c[2]"), BigintRange.of(0, 4, false))))
                                .build()));

        // arrays of arrays
        tester.assertRoundTripWithSettings(arrayType(arrayType(INTEGER)),
                createList(NUM_ROWS, i -> nCopies(1 + random.nextInt(5), ImmutableList.of(1, 2, 3))),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1][1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][2]", "c[4][2]", "c[5][3]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][3]", "c[10][2]", "c[3][10]").build()));

        // arrays of maps
        tester.assertRoundTripWithSettings(arrayType(mapType(INTEGER, INTEGER)),
                createList(NUM_ROWS, i -> nCopies(5, ImmutableMap.of(1, 10, 2, 20))),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1][1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][1]", "c[4][1]", "c[3][2]").build()));
    }

    @Test
    public void testArrayIndexOutOfBounds()
            throws Exception
    {
        Random random = new Random(0);

        // non-null arrays of varying sizes
        try {
            tester.testRoundTrip(arrayType(INTEGER),
                    createList(NUM_ROWS, i -> randomIntegers(random.nextInt(10), random)),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // non-null nested arrays of varying sizes
        try {
            tester.testRoundTrip(arrayType(arrayType(INTEGER)),
                    createList(NUM_ROWS, i -> ImmutableList.of(randomIntegers(random.nextInt(5), random), randomIntegers(random.nextInt(5), random))),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2][3]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // empty arrays
        try {
            tester.testRoundTrip(arrayType(INTEGER),
                    nCopies(NUM_ROWS, ImmutableList.of()),
                    ImmutableList.of(ImmutableMap.of(new Subfield("c[2]"), IS_NULL)));
            fail("Expected 'Array subscript out of bounds' exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().contains("Array subscript out of bounds"));
        }

        // empty nested arrays
        try {
            tester.testRoundTrip(arrayType(arrayType(INTEGER)),
                    nCopies(NUM_ROWS, ImmutableList.of()),
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
        for (Type type : ImmutableList.of(BOOLEAN, BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, TIMESTAMP, DECIMAL_TYPE_PRECISION_19, DECIMAL_TYPE_PRECISION_4, VARCHAR, CHAR_10, VARBINARY, arrayType(INTEGER))) {
            tester.testRoundTrip(arrayType(type),
                    nCopies(NUM_ROWS, nCopies(5, null)),
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
                        createList(NUM_ROWS, i -> random.nextInt()),
                        createList(NUM_ROWS, i -> ImmutableList.of(random.nextInt(), random.nextBoolean()))),
                ImmutableList.of(
                        ImmutableMap.of(0, toSubfieldFilter(BigintRange.of(0, Integer.MAX_VALUE, false))),
                        ImmutableMap.of(1, toSubfieldFilter(IS_NULL)),
                        ImmutableMap.of(1, toSubfieldFilter(IS_NOT_NULL)),
                        ImmutableMap.of(1, toSubfieldFilter("c.field_0", BigintRange.of(0, Integer.MAX_VALUE, false))),
                        ImmutableMap.of(1, toSubfieldFilter("c.field_0", IS_NULL))));

        tester.testRoundTripTypes(ImmutableList.of(rowType(INTEGER, BOOLEAN), INTEGER),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> i % 7 == 0 ? null : ImmutableList.of(random.nextInt(), random.nextBoolean())),
                        createList(NUM_ROWS, i -> i % 11 == 0 ? null : random.nextInt())),
                ImmutableList.of(
                        ImmutableMap.of(0, toSubfieldFilter(IS_NOT_NULL), 1, toSubfieldFilter(IS_NULL)),
                        ImmutableMap.of(0, toSubfieldFilter("c.field_0", BigintRange.of(0, Integer.MAX_VALUE, false))),
                        ImmutableMap.of(0, toSubfieldFilter("c.field_0", BigintRange.of(0, Integer.MAX_VALUE, true)))));
    }

    @Test
    public void testMaps()
            throws Exception
    {
        Random random = new Random(0);

        tester.testRoundTrip(mapType(INTEGER, INTEGER), createList(NUM_ROWS, i -> createMap(i)));

        // map column with no nulls
        tester.testRoundTripTypes(
                ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> random.nextInt()),
                        createList(NUM_ROWS, i -> createMap(i))),
                toSubfieldFilters(
                        ImmutableMap.of(0, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(1, IS_NOT_NULL),
                        ImmutableMap.of(1, IS_NULL)));

        // map column with nulls
        tester.testRoundTripTypes(
                ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> random.nextInt()),
                        createList(NUM_ROWS, i -> i % 5 == 0 ? null : createMap(i))),
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
                        createList(NUM_ROWS, i -> i % 5 == 0 ? null : createMap(i)),
                        createList(NUM_ROWS, i -> random.nextInt())),
                toSubfieldFilters(
                        ImmutableMap.of(0, IS_NULL, 1, BigintRange.of(0, Integer.MAX_VALUE, false)),
                        ImmutableMap.of(0, IS_NOT_NULL, 1, BigintRange.of(0, Integer.MAX_VALUE, false))));

        // empty maps
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> random.nextInt()),
                        Collections.nCopies(NUM_ROWS, ImmutableMap.of())),
                ImmutableList.of());

        // read selected positions from all nulls map column
        tester.testRoundTripTypes(ImmutableList.of(INTEGER, mapType(INTEGER, INTEGER)),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> random.nextInt(10)),
                        createList(NUM_ROWS, i -> null)),
                toSubfieldFilters(ImmutableMap.of(0, BigintRange.of(0, 5, false))));
    }

    @Test
    public void testMapsWithSubfieldPruning()
            throws Exception
    {
        tester.assertRoundTripWithSettings(mapType(INTEGER, INTEGER),
                createList(NUM_ROWS, i -> ImmutableMap.of(1, 10, 2, 20)),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[10]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2]", "c[10]").build()));

        // maps of maps
        tester.assertRoundTripWithSettings(mapType(INTEGER, mapType(INTEGER, INTEGER)),
                createList(NUM_ROWS, i -> ImmutableMap.of(1, createMap(i), 2, createMap(i + 1))),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1][1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][2]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][1]", "c[10][1]").build()));

        // maps of arrays
        tester.assertRoundTripWithSettings(mapType(INTEGER, arrayType(INTEGER)),
                createList(NUM_ROWS, i -> ImmutableMap.of(1, nCopies(5, 10), 2, nCopies(5, 20))),
                ImmutableList.of(
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[1][1]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][2]", "c[2][3]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][2]", "c[10][3]", "c[2][10]").build(),
                        OrcReaderSettings.builder().addRequiredSubfields(0, "c[2][2]", "c[1][2]").build()));
    }

    private static Map<Integer, Integer> createMap(int seed)
    {
        int mapSize = Math.abs(seed) % 7 + 1;
        return IntStream.range(0, mapSize).boxed().collect(toImmutableMap(Function.identity(), i -> i + seed));
    }

    private static <T> List<T> createList(int size, Function<Integer, T> createElement)
    {
        return IntStream.range(0, size).mapToObj(createElement::apply).collect(toList());
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

    @Test
    public void testVarchars()
            throws Exception
    {
        Random random = new Random(0);
        tester.testRoundTripTypes(
                ImmutableList.of(VARCHAR, VARCHAR, VARCHAR),
                ImmutableList.of(newArrayList("abc", "def", null, "hij", "klm"), newArrayList(null, null, null, null, null), newArrayList("abc", "def", null, null, null)),
                toSubfieldFilters(ImmutableMap.of(
                        0, stringIn(true, "abc", "def"),
                        1, stringIn(true, "10", "11"),
                        2, stringIn(true, "def", "abc"))));

        // dictionary
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), NUM_ROWS)),
                stringIn(false, "apple", "apple pie"));

        // direct
        tester.testRoundTrip(VARCHAR,
                intsBetween(0, NUM_ROWS).stream().map(Object::toString).collect(toList()),
                stringIn(false, "10", "11"),
                stringIn(true, "10", "11"),
                stringBetween(false, "14", "10"));

        // direct & dictionary with filters
        tester.testRoundTripTypes(
                ImmutableList.of(VARCHAR, VARCHAR),
                ImmutableList.of(
                        intsBetween(0, NUM_ROWS).stream().map(Object::toString).collect(toList()),
                        newArrayList(limit(cycle(ImmutableList.of("A", "B", "C")), NUM_ROWS))),
                toSubfieldFilters(ImmutableMap.of(
                        0, stringBetween(true, "16", "10"),
                        1, stringBetween(false, "B", "A"))));

        //stripe dictionary
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));

        //empty sequence
        tester.testRoundTrip(VARCHAR, nCopies(NUM_ROWS, ""), stringEquals(false, ""));

        // copy of AbstractOrcTester::testDwrfInvalidCheckpointsForRowGroupDictionary
        List<Integer> values = newArrayList(limit(
                cycle(concat(
                        ImmutableList.of(1), nCopies(9999, 123),
                        ImmutableList.of(2), nCopies(9999, 123),
                        ImmutableList.of(3), nCopies(9999, 123),
                        nCopies(1_000_000, null))),
                200_000));

        tester.assertRoundTrip(
                VARCHAR,
                newArrayList(values).stream()
                        .map(value -> value == null ? null : String.valueOf(value))
                        .collect(toList()));

        //copy of AbstractOrcTester::testDwrfInvalidCheckpointsForStripeDictionary
        tester.testRoundTrip(
                VARCHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 200_000)).stream()
                        .map(Object::toString)
                        .collect(toList()));

        // presentStream is null in some row groups
        Function<Integer, String> randomStrings = i -> String.valueOf(random.nextInt(NUM_ROWS));
        tester.testRoundTripTypes(
                ImmutableList.of(INTEGER, VARCHAR),
                ImmutableList.of(createList(NUM_ROWS, i -> random.nextInt(NUM_ROWS)), newArrayList(createList(NUM_ROWS, randomStrings))),
                toSubfieldFilters(ImmutableMap.of(0, BigintRange.of(10_000, 15_000, true))));

        // dataStream is null and lengths are 0
        tester.testRoundTrip(VARCHAR, newArrayList("", null), toSubfieldFilters(stringNotEquals(true, "")));
    }

    @Test
    public void testChars()
            throws Exception
    {
        // multiple columns filter on not null
        tester.testRoundTripTypes(ImmutableList.of(VARCHAR, createCharType(5)),
                ImmutableList.of(
                        newArrayList(limit(cycle(ImmutableList.of("123456789", "23456789", "3456789")), NUM_ROWS)),
                        newArrayList(limit(cycle(ImmutableList.of("12345", "23456", "34567")), NUM_ROWS))),
                toSubfieldFilters(ImmutableMap.of(0, IS_NOT_NULL)));

        tester.testRoundTrip(createCharType(2), newArrayList(limit(cycle(ImmutableList.of("aa", "bb", "cc", "dd")), NUM_ROWS)), IS_NULL);

        tester.testRoundTrip(createCharType(1), newArrayList(limit(cycle(ImmutableList.of("a", "b", "c", "d")), NUM_ROWS)),
                stringIn(false, "a", "b"),
                stringIn(true, "a", "b"));

        // char with padding
        tester.testRoundTrip(
                CHAR_10,
                intsBetween(0, NUM_ROWS).stream()
                        .map(i -> toCharValue(i, 10))
                        .collect(toList()));

        // char with filter
        tester.testRoundTrip(
                CHAR_10,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), NUM_ROWS)).stream()
                        .map(i -> toCharValue(i, 10))
                        .collect(toList()),
                stringIn(true, toCharValue(1, 10), toCharValue(3, 10)));

        // char with 0 truncated length
        tester.testRoundTrip(CHAR_10, newArrayList(limit(cycle(toCharValue("", 10)), NUM_ROWS)));

        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))),
                stringIn(false, "a", "b"));
    }

    @Test
    public void testVarBinaries()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY,
                createList(NUM_ROWS, i -> new SqlVarbinary(String.valueOf(i).getBytes(UTF_8))),
                stringIn(false, "10", "11"));

        tester.testRoundTripTypes(
                ImmutableList.of(VARBINARY, VARBINARY),
                ImmutableList.of(
                        createList(NUM_ROWS, i -> new SqlVarbinary(Ints.toByteArray(i))),
                        Streams.stream(limit(cycle(ImmutableList.of("A", "B", "C")), NUM_ROWS))
                                .map(String::getBytes)
                                .map(SqlVarbinary::new)
                                .collect(toImmutableList())),
                toSubfieldFilters(ImmutableMap.of(
                        0, stringBetween(true, "10", "16"),
                        1, stringBetween(false, "A", "B"))));

        tester.testRoundTrip(
                VARBINARY,
                createList(NUM_ROWS, i -> new SqlVarbinary(String.valueOf(i).getBytes(UTF_8))),
                bytesBetween(false, new byte[] {8}, new byte[] {9}));

        tester.testRoundTrip(
                VARBINARY,
                nCopies(NUM_ROWS, new SqlVarbinary(new byte[0])),
                bytesBetween(false, new byte[0], new byte[] {1}));

        tester.testRoundTrip(
                VARBINARY,
                ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), NUM_ROWS)).stream()
                        .map(String::valueOf)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toImmutableList()),
                bytesBetween(false, new byte[] {1}, new byte[] {12}));
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
                .map(timestamp -> sqlTimestampOf(timestamp & Integer.MAX_VALUE, SESSION))
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

    private static TupleDomainFilter stringBetween(boolean nullAllowed, String upper, String lower)
    {
        return BytesRange.of(lower.getBytes(), false, upper.getBytes(), false, nullAllowed);
    }

    private static TupleDomainFilter stringEquals(boolean nullAllowed, String value)
    {
        return BytesRange.of(value.getBytes(), false, value.getBytes(), false, nullAllowed);
    }

    private static TupleDomainFilter stringNotEquals(boolean nullAllowed, String value)
    {
        return TupleDomainFilter.MultiRange.of(ImmutableList.of(
                BytesRange.of(null, false, value.getBytes(), true, nullAllowed),
                BytesRange.of(value.getBytes(), true, null, false, nullAllowed)), nullAllowed, false);
    }

    private static TupleDomainFilter stringIn(boolean nullAllowed, String... values)
    {
        return BytesValues.of(Arrays.stream(values).map(String::getBytes).toArray(byte[][]::new), nullAllowed);
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static BytesRange bytesBetween(boolean nullAllowed, byte[] lower, byte[] upper)
    {
        return BytesRange.of(lower, false, upper, false, nullAllowed);
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

    private static String toCharValue(Object value, int minLength)
    {
        return Strings.padEnd(value.toString(), minLength, ' ');
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

    private static List<Integer> randomIntegers(int size, Random random)
    {
        return createList(size, i -> random.nextInt());
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

    private static List<Long> varintScaleSequence(int rows)
    {
        List<Long> values = new ArrayList();
        long[] numbers = new long[] {1L, 1L << 8, 1L << 13, 1L << 20, 1L << 27, 1L << 34, 1L << 40, 1L << 47, 1L << 53, 1L << 60, 1L << 63};
        for (int i = 0; i < rows; i++) {
            values.add(numbers[i % numbers.length] + i);
            values.add(-numbers[i % numbers.length] + i);
        }
        return values;
    }

    private static List<Byte> toByteArray(List<Integer> integers)
    {
        return integers.stream().map((i) -> i == null ? null : i.byteValue()).collect(toList());
    }
}
