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

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Shorts;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.google.common.base.Functions.compose;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
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
        tester.testRoundTrip(javaBooleanObjectInspector, limit(cycle(ImmutableList.of(true, false, false)), 30_000));
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
        testRoundTripNumeric(concat(ImmutableList.of(1), Collections.nCopies(9999, 123), ImmutableList.of(2), Collections.nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<Integer> writeValues)
            throws Exception
    {
        tester.testRoundTrip(javaByteObjectInspector, transform(writeValues, intToByte()), byteToLong());
        tester.testRoundTrip(javaShortObjectInspector, transform(writeValues, intToShort()), shortToLong());
        tester.testRoundTrip(javaIntObjectInspector, writeValues, intToLong());
        tester.testRoundTrip(javaLongObjectInspector, transform(writeValues, intToLong()));
        tester.testRoundTrip(javaTimestampObjectInspector, transform(writeValues, intToTimestamp()), timestampToLong(), timestampToString());

        // date has three representations
        // normal format: DAYS since 1970
        // json stack: is milliseconds UTC since 1970
        // json json: ISO 8601 format date
        tester.testRoundTrip(javaDateObjectInspector,
                transform(writeValues, intToDate()),
                transform(writeValues, intToDays()),
                transform(writeValues, intToDaysMillis()),
                transform(writeValues, intToDateString()));
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, floatSequence(0.0f, 0.1f, 30_000), floatToDouble());
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, transform(intsBetween(0, 30_000), toStringFunction()));
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), toStringFunction())), 30_000));
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, concat(ImmutableList.of("a"), Collections.nCopies(9999, "123"), ImmutableList.of("b"), Collections.nCopies(9999, "123")));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(""), 30_000));
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = transform(intsBetween(0, 30_000), compose(stringToByteArray(), toStringFunction()));
        tester.testRoundTrip(javaByteArrayObjectInspector,
                writeValues,
                transform(writeValues, byteArrayToString()),
                transform(writeValues, byteArrayToBase64()),
                transform(writeValues, byteArrayToBase64()));
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), compose(stringToByteArray(), toStringFunction()))), 30_000);
        tester.testRoundTrip(javaByteArrayObjectInspector,
                writeValues,
                transform(writeValues, byteArrayToString()),
                transform(writeValues, byteArrayToBase64()),
                transform(writeValues, byteArrayToBase64()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaByteArrayObjectInspector, limit(cycle(new byte[0]), 30_000), byteArrayToString());
    }

    private static <T> Iterable<T> skipEvery(final int n, final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new AbstractIterator<T>()
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
        };
    }

    private static <T> Iterable<T> repeatEach(final int n, final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new AbstractIterator<T>()
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
        };
    }

    private static Iterable<Float> floatSequence(double start, double step, int items)
    {
        return transform(doubleSequence(start, step, items), new Function<Double, Float>()
        {
            @Nullable
            @Override
            public Float apply(@Nullable Double input)
            {
                if (input == null) {
                    return null;
                }
                return input.floatValue();
            }
        });
    }

    private static Iterable<Double> doubleSequence(final double start, final double step, final int items)
    {
        return new Iterable<Double>()
        {
            @Override
            public Iterator<Double> iterator()
            {
                return new AbstractSequentialIterator<Double>(start)
                {
                    private int item;

                    @Override
                    protected Double computeNext(Double previous)
                    {
                        if (item >= items) {
                            return null;
                        }
                        item++;
                        return previous + step;
                    }
                };
            }
        };
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static Function<Float, Double> floatToDouble()
    {
        return new Function<Float, Double>()
        {
            @Nullable
            @Override
            public Double apply(@Nullable Float input)
            {
                if (input == null) {
                    return null;
                }
                return input.doubleValue();
            }
        };
    }

    private static Function<Integer, Byte> intToByte()
    {
        return new Function<Integer, Byte>()
        {
            @Nullable
            @Override
            public Byte apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                return input.byteValue();
            }
        };
    }

    private static Function<Integer, Short> intToShort()
    {
        return new Function<Integer, Short>()
        {
            @Nullable
            @Override
            public Short apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                return Shorts.checkedCast(input);
            }
        };
    }

    private static Function<Byte, Long> byteToLong()
    {
        return toLong();
    }

    private static Function<Short, Long> shortToLong()
    {
        return toLong();
    }

    private static Function<Integer, Long> intToLong()
    {
        return toLong();
    }

    private static <N extends Number> Function<N, Long> toLong()
    {
        return new Function<N, Long>()
        {
            @Nullable
            @Override
            public Long apply(@Nullable N input)
            {
                if (input == null) {
                    return null;
                }
                return input.longValue();
            }
        };
    }

    private static Function<Integer, Timestamp> intToTimestamp()
    {
        return new Function<Integer, Timestamp>()
        {
            @Nullable
            @Override
            public Timestamp apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                Timestamp timestamp = new Timestamp(0);
                long seconds = (input / 1000);
                int nanos = ((input % 1000) * 1_000_000);

                // add some junk nanos to the timestamp, which will be truncated
                nanos += 888_8888;

                if (nanos < 0) {
                    nanos += 1_000_000_000;
                    seconds -= 1;
                }
                if (nanos > 1_000_000_000) {
                    nanos -= 1_000_000_000;
                    seconds += 1;
                }
                timestamp.setTime(seconds);
                timestamp.setNanos(nanos);
                return timestamp;
            }
        };
    }

    private static Function<Timestamp, Long> timestampToLong()
    {
        return new Function<Timestamp, Long>()
        {
            @Nullable
            @Override
            public Long apply(@Nullable Timestamp input)
            {
                if (input == null) {
                    return null;
                }
                return input.getTime();
            }
        };
    }

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(HIVE_STORAGE_TIME_ZONE);

    private static Function<Timestamp, String> timestampToString()
    {
        return new Function<Timestamp, String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable Timestamp input)
            {
                if (input == null) {
                    return null;
                }
                return TIMESTAMP_FORMATTER.print(input.getTime());
            }
        };
    }

    private static Function<Integer, Date> intToDate()
    {
        return new Function<Integer, Date>()
        {
            @Nullable
            @Override
            public Date apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                Date date = new Date(0);
                date.setTime(TimeUnit.DAYS.toMillis(input));
                return date;
            }
        };
    }

    private static Function<Integer, Long> intToDays()
    {
        return new Function<Integer, Long>()
        {
            @Nullable
            @Override
            public Long apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                return (long) input;
            }
        };
    }

    private static Function<Integer, Long> intToDaysMillis()
    {
        return new Function<Integer, Long>()
        {
            @Nullable
            @Override
            public Long apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                return TimeUnit.DAYS.toMillis(input);
            }
        };
    }

    private static Function<Integer, String> intToDateString()
    {
        return new Function<Integer, String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable Integer input)
            {
                if (input == null) {
                    return null;
                }
                return ISODateTimeFormat.date().withZoneUTC().print(TimeUnit.DAYS.toMillis(input));
            }
        };
    }

    private static Function<String, byte[]> stringToByteArray()
    {
        return new Function<String, byte[]>()
        {
            @Override
            public byte[] apply(String input)
            {
                return input.getBytes(UTF_8);
            }
        };
    }

    private static Function<byte[], String> byteArrayToBase64()
    {
        return new Function<byte[], String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable byte[] input)
            {
                if (input == null) {
                    return null;
                }
                return BaseEncoding.base64().encode(input);
            }
        };
    }

    private static Function<byte[], String> byteArrayToString()
    {
        return new Function<byte[], String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable byte[] input)
            {
                if (input == null) {
                    return null;
                }
                return new String(input, UTF_8);
            }
        };
    }
}
