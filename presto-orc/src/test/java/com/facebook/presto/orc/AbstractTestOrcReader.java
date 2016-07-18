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

import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestOrcReader
{
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_2 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(2, 1));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_4 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(4, 2));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_8 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(8, 4));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_17 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(17, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_18 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(18, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_38 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(38, 16));

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);

    private final OrcTester tester;

    public AbstractTestOrcReader(OrcTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(javaBooleanObjectInspector, limit(cycle(ImmutableList.of(true, false, false)), 30_000), BOOLEAN);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), ImmutableList.of(30_000, 20_000))), 30_000));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());
        tester.testRoundTrip(
                javaByteObjectInspector,
                writeValues.stream()
                        .map(value -> (long) value.byteValue()) // truncate values to byte range
                        .collect(toList()),
                BIGINT);

        tester.testRoundTrip(
                javaShortObjectInspector,
                writeValues.stream()
                        .map(value -> (long) value.shortValue()) // truncate values to short range
                        .collect(toList()),
                BIGINT);

        tester.testRoundTrip(javaIntObjectInspector, writeValues, BIGINT);
        tester.testRoundTrip(javaLongObjectInspector, writeValues, BIGINT);

        tester.testRoundTrip(
                javaDateObjectInspector,
                writeValues.stream()
                        .map(Long::intValue)
                        .map(SqlDate::new)
                        .collect(toList()),
                DATE);

        tester.testRoundTrip(
                javaTimestampObjectInspector,
                writeValues.stream()
                        .map(timestamp -> new SqlTimestamp(timestamp, UTC_KEY))
                        .collect(toList()),
                TIMESTAMP);
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, doubleSequence(0.0, 0.1, 30_000), DOUBLE);
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000), DOUBLE);
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_2, decimalSequence("0", "1", 30, 2, 1), DECIMAL_TYPE_PRECISION_2);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_4, decimalSequence("0", "1", 30_00, 4, 2), DECIMAL_TYPE_PRECISION_4);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_8, decimalSequence("0", "100", 30_000, 8, 4), DECIMAL_TYPE_PRECISION_8);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_17, decimalSequence("0", "1000000", 30_000, 17, 8), DECIMAL_TYPE_PRECISION_17);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_18, decimalSequence("0", "1000000", 30_000, 18, 8), DECIMAL_TYPE_PRECISION_18);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_38, decimalSequence("0", "100000000000000", 30_000, 38, 16), DECIMAL_TYPE_PRECISION_38);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);

        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000), VARCHAR);
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, transform(intsBetween(0, 30_000), Object::toString), VARCHAR);
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), Object::toString)), 30_000), VARCHAR);
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123")), VARCHAR);
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(""), 30_000), VARCHAR);
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()),
                VARBINARY);
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()),
                VARBINARY);
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaByteArrayObjectInspector, nCopies(30_000, new SqlVarbinary(new byte[0])), VARBINARY);
    }

    @Test
    public void testDwrfInvalidCheckpointsForRowGroupDictionary()
            throws Exception
    {
        Iterable<Integer> values = limit(cycle(concat(
                        ImmutableList.of(1), nCopies(9999, 123),
                        ImmutableList.of(2), nCopies(9999, 123),
                        ImmutableList.of(3), nCopies(9999, 123),
                        nCopies(1_000_000, null))),
                200_000);

        tester.assertRoundTrip(javaIntObjectInspector, transform(values, value -> value == null ? null : (long) value), BIGINT);

        Iterable<String> stringValue = transform(values, value -> value == null ? null : String.valueOf(value));
        tester.assertRoundTrip(javaStringObjectInspector, stringValue, VARCHAR);
    }

    @Test
    public void testDwrfInvalidCheckpointsForStripeDictionary()
            throws Exception
    {
        Iterable<String> values = limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), Object::toString)), 200_000);
        tester.testRoundTrip(javaStringObjectInspector, values, VARCHAR);
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
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
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

//    private static Iterable<HiveDecimal> decimalSequence(String start, String step, int items, int scale)
//    {
//        HiveDecimal hiveStep = HiveDecimal.create(step);
//        return () -> new AbstractSequentialIterator<HiveDecimal>(HiveDecimal.create(start))
//        {
//            private int item;
//
//            @Override
//            protected HiveDecimal computeNext(HiveDecimal previous)
//            {
//                if (item >= items) {
//                    return null;
//                }
//                item++;
//                return previous.add(hiveStep).setScale(scale);
//            }
//        };
//    }

    private static Function<HiveDecimal, SqlDecimal> toSqlDecimal(int scale)
    {
        return hiveDecimal -> new SqlDecimal(hiveDecimal.unscaledValue(), hiveDecimal.precision(), scale);
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }
}
