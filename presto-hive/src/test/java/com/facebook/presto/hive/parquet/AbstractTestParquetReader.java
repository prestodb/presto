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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlVarbinary;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.primitives.Shorts;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.parquet.ParquetTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.hive.parquet.ParquetTester.insertNullEvery;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class AbstractTestParquetReader
{
    protected static final int MAX_PRECISION_INT32 = (int) AbstractTestParquetReader.maxPrecision(4);
    protected static final int MAX_PRECISION_INT64 = (int) AbstractTestParquetReader.maxPrecision(8);
    protected final ParquetTester tester;
    protected Logger parquetLogger;

    public AbstractTestParquetReader(ParquetTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);

        // Parquet has excessive logging at INFO level
        parquetLogger = Logger.getLogger("org.apache.parquet.hadoop");
        parquetLogger.setLevel(Level.WARNING);
    }

    // copied from Parquet code to determine the max decimal precision supported by INT32/INT64
    private static long maxPrecision(int numBytes)
    {
        return Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
    }

    protected static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
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

    protected static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
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

    protected static Iterable<Float> floatSequence(double start, double step, int items)
    {
        return transform(AbstractTestParquetReader.doubleSequence(start, step, items), input -> {
            if (input == null) {
                return null;
            }
            return input.floatValue();
        });
    }

    protected static Iterable<Double> doubleSequence(double start, double step, int items)
    {
        return () -> new AbstractSequentialIterator<Double>(start)
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

    protected static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    protected static ContiguousSet<Long> longsBetween(long lowerInclusive, long upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.longs());
    }

    protected static ContiguousSet<BigInteger> bigIntegersBetween(BigInteger lowerInclusive, BigInteger upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.bigIntegers());
    }

    protected static Byte intToByte(Integer input)
    {
        if (input == null) {
            return null;
        }
        return input.byteValue();
    }

    protected static Short intToShort(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Shorts.checkedCast(input);
    }

    protected static Integer byteToInt(Byte input)
    {
        return AbstractTestParquetReader.toInteger(input);
    }

    protected static Integer shortToInt(Short input)
    {
        return AbstractTestParquetReader.toInteger(input);
    }

    protected static Long intToLong(Integer input)
    {
        return AbstractTestParquetReader.toLong(input);
    }

    protected static Integer longToInt(Long input)
    {
        return AbstractTestParquetReader.toInteger(input);
    }

    private static <N extends Number> Integer toInteger(N input)
    {
        if (input == null) {
            return null;
        }
        return input.intValue();
    }

    static <N extends Number> Long toLong(N input)
    {
        if (input == null) {
            return null;
        }
        return input.longValue();
    }

    protected static byte[] stringToByteArray(String input)
    {
        return input.getBytes(UTF_8);
    }

    protected static SqlVarbinary byteArrayToVarbinary(byte[] input)
    {
        if (input == null) {
            return null;
        }
        return new SqlVarbinary(input);
    }

    protected static Timestamp intToTimestamp(Integer input)
    {
        if (input == null) {
            return null;
        }
        Timestamp timestamp = new Timestamp(0);
        long seconds = (input / 1000);
        int nanos = ((input % 1000) * 1_000_000);

        // add some junk nanos to the timestamp, which will be truncated
        nanos += 888_888;

        if (nanos < 0) {
            nanos += 1_000_000_000;
            seconds -= 1;
        }
        if (nanos > 1_000_000_000) {
            nanos -= 1_000_000_000;
            seconds += 1;
        }
        timestamp.setTime(seconds * 1000);
        timestamp.setNanos(nanos);
        return timestamp;
    }

    protected static SqlTimestamp intToSqlTimestamp(Integer input)
    {
        if (input == null) {
            return null;
        }
        return sqlTimestampOf(input, SESSION);
    }

    protected static Date intToDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Date.valueOf(LocalDate.ofEpochDay(input));
    }

    protected static SqlDate intToSqlDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return new SqlDate(input);
    }

    protected <F> List<List> createTestStructs(Iterable<F> fieldValues)
    {
        checkArgument(fieldValues.iterator().hasNext(), "struct field values cannot be empty");
        List<List> structs = new ArrayList<>();
        for (F field : fieldValues) {
            structs.add(singletonList(field));
        }
        return structs;
    }

    protected List<List> createTestStructs(Iterable<?>... values)
    {
        List<List> structs = new ArrayList<>();
        List<Iterator> iterators = Arrays.stream(values).map(Iterable::iterator).collect(Collectors.toList());
        iterators.forEach(iter -> checkArgument(iter.hasNext(), "struct field values cannot be empty"));
        while (iterators.stream().allMatch(Iterator::hasNext)) {
            structs.add(iterators.stream().map(Iterator::next).collect(Collectors.toList()));
        }
        return structs;
    }

    protected Iterable<List> createNullableTestStructs(Iterable<?>... values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestStructs(values));
    }

    protected <T> List<List<T>> createTestArrays(Iterable<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        Iterator<T> valuesIter = values.iterator();
        List<T> array = new ArrayList<>();
        while (valuesIter.hasNext()) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(valuesIter.next());
        }
        return arrays;
    }

    protected <T> Iterable<List<T>> createNullableTestArrays(Iterable<T> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestArrays(values));
    }

    protected <T> List<List<T>> createFixedTestArrays(Iterable<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        Iterator<T> valuesIter = values.iterator();
        List<T> array = new ArrayList<>();
        int count = 1;
        while (valuesIter.hasNext()) {
            if (count % 10 == 0) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (count % 20 == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(valuesIter.next());
            ++count;
        }
        return arrays;
    }

    protected <K, V> Iterable<Map<K, V>> createFixedTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Iterator<K> keysIterator = keys.iterator();
        Iterator<V> valuesIterator = values.iterator();
        Map<K, V> map = new HashMap<>();
        int count = 1;
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            if (count % 5 == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (count % 10 == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keysIterator.next(), valuesIterator.next());
            ++count;
        }
        return maps;
    }

    protected <K, V> Iterable<Map<K, V>> createTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Iterator<K> keysIterator = keys.iterator();
        Iterator<V> valuesIterator = values.iterator();
        Map<K, V> map = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keysIterator.next(), valuesIterator.next());
        }
        return maps;
    }

    protected <K, V> Iterable<Map<K, V>> createNullableTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestMaps(keys, values));
    }
}
