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

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.cache.CachingOrcFileTailSource;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.createCustomOrcRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcRecordWriter;
import static com.facebook.presto.orc.OrcTester.createSettableStructObjectInspector;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestOrcReader
{
    private static final int CHAR_LENGTH = 10;

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);
    private static final CharType CHAR = createCharType(CHAR_LENGTH);

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
        tester.testRoundTrip(BOOLEAN, newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)));
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testNegativeLongSequence()
            throws Exception
    {
        // A flaw in ORC encoding makes it impossible to represent timestamp
        // between 1969-12-31 23:59:59.000, exclusive, and 1970-01-01 00:00:00.000, exclusive.
        // Therefore, such data won't round trip and are skipped from test.
        testRoundTripNumeric(intsBetween(-31_234, -999));
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
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), intsBetween(0, 18), ImmutableList.of(30_000, 20_000, 400_000, 30_000, 20_000))), 30_000));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)));
    }

    @Test
    public void testCaching()
            throws Exception
    {
        Cache<OrcDataSourceId, OrcFileTail> orcFileTailCache = CacheBuilder.newBuilder()
                .maximumWeight(new DataSize(1, MEGABYTE).toBytes())
                .weigher((id, tail) -> ((OrcFileTail) tail).getFooterSize() + ((OrcFileTail) tail).getMetadataSize())
                .expireAfterAccess(new Duration(10, MINUTES).toMillis(), MILLISECONDS)
                .recordStats()
                .build();
        OrcFileTailSource orcFileTailSource = new CachingOrcFileTailSource(new StorageOrcFileTailSource(), orcFileTailCache);

        Cache<StripeId, Slice> stripeFootercache = CacheBuilder.newBuilder()
                .maximumWeight(new DataSize(1, MEGABYTE).toBytes())
                .weigher((id, footer) -> ((Slice) footer).length())
                .expireAfterAccess(new Duration(10, MINUTES).toMillis(), MILLISECONDS)
                .recordStats()
                .build();
        Cache<StripeStreamId, Slice> stripeStreamCache = CacheBuilder.newBuilder()
                .maximumWeight(new DataSize(1, MEGABYTE).toBytes())
                .weigher((id, stream) -> ((Slice) stream).length())
                .expireAfterAccess(new Duration(10, MINUTES).toMillis(), MILLISECONDS)
                .recordStats()
                .build();
        StripeMetadataSource stripeMetadataSource = new CachingStripeMetadataSource(new StorageStripeMetadataSource(), stripeFootercache, stripeStreamCache);

        try (TempFile tempFile = createTempFile()) {
            OrcBatchRecordReader storageReader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, ImmutableList.of(BIGINT), INITIAL_BATCH_SIZE, orcFileTailSource, stripeMetadataSource, true, ImmutableMap.of(), false);
            assertEquals(orcFileTailCache.stats().missCount(), 1);
            assertEquals(orcFileTailCache.stats().hitCount(), 0);

            OrcBatchRecordReader cacheReader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, ImmutableList.of(BIGINT), INITIAL_BATCH_SIZE, orcFileTailSource, stripeMetadataSource, true, ImmutableMap.of(), false);
            assertEquals(orcFileTailCache.stats().missCount(), 1);
            assertEquals(orcFileTailCache.stats().hitCount(), 1);

            assertEquals(storageReader.getRetainedSizeInBytes(), cacheReader.getRetainedSizeInBytes());
            assertEquals(storageReader.getFileRowCount(), cacheReader.getFileRowCount());
            assertEquals(storageReader.getSplitLength(), cacheReader.getSplitLength());

            storageReader.nextBatch();
            assertEquals(stripeFootercache.stats().missCount(), 1);
            assertEquals(stripeFootercache.stats().hitCount(), 0);
            assertEquals(stripeStreamCache.stats().missCount(), 2);
            assertEquals(stripeStreamCache.stats().hitCount(), 0);
            cacheReader.nextBatch();
            assertEquals(stripeFootercache.stats().missCount(), 1);
            assertEquals(stripeFootercache.stats().hitCount(), 1);
            assertEquals(stripeStreamCache.stats().missCount(), 2);
            assertEquals(stripeStreamCache.stats().hitCount(), 2);
            assertEquals(storageReader.readBlock(0).getInt(0), cacheReader.readBlock(0).getInt(0));
        }
    }

    private static TempFile createTempFile()
            throws IOException, SerDeException
    {
        TempFile file = new TempFile();
        RecordWriter writer = createOrcRecordWriter(file.getFile(), ORC_12, CompressionKind.NONE, BIGINT);

        @SuppressWarnings("deprecation") Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        objectInspector.setStructFieldData(row, field, 1L);
        Writable record = serde.serialize(row, objectInspector);
        writer.write(record);

        writer.close(false);
        return file;
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());
        tester.testRoundTrip(
                TINYINT,
                writeValues.stream()
                        .map(Long::byteValue) // truncate values to byte range
                        .collect(toList()));

        tester.testRoundTrip(
                SMALLINT,
                writeValues.stream()
                        .map(Long::shortValue) // truncate values to short range
                        .collect(toList()));

        tester.testRoundTrip(
                INTEGER,
                writeValues.stream()
                        .map(Long::intValue) // truncate values to int range
                        .collect(toList()));

        tester.testRoundTrip(BIGINT, writeValues);

        tester.testRoundTrip(
                DATE,
                writeValues.stream()
                        .map(Long::intValue)
                        .map(SqlDate::new)
                        .collect(toList()));

        tester.testRoundTrip(
                TIMESTAMP,
                writeValues.stream()
                        .map(timestamp -> sqlTimestampOf(timestamp, SESSION))
                        .collect(toList()));
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(REAL, floatSequence(0.0f, 0.1f, 30_000));
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(REAL, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f));
        tester.testRoundTrip(REAL, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));

        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -0.0f, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_8, decimalSequence("-3000000", "100", 60_000, 8, 4));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_17, decimalSequence("-30000000000", "1000000", 60_000, 17, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, decimalSequence("-30000000000", "1000000", 60_000, 18, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, decimalSequence("-3000000000000000000", "100000000000000", 60_000, 38, 16));
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));

        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000)));
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(""), 30_000)));
    }

    @Test
    public void testCharDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                intsBetween(0, 30_000).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testCharDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testEmptyCharSequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR, newArrayList(limit(cycle("          "), 30_000)));
    }

    private String toCharValue(Object value)
    {
        return Strings.padEnd(value.toString(), CHAR_LENGTH, ' ');
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY, ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(VARBINARY, nCopies(30_000, new SqlVarbinary(new byte[0])));
    }

    @Test
    public void testDwrfInvalidCheckpointsForRowGroupDictionary()
            throws Exception
    {
        List<Integer> values = newArrayList(limit(
                cycle(concat(
                        ImmutableList.of(1), nCopies(9999, 123),
                        ImmutableList.of(2), nCopies(9999, 123),
                        ImmutableList.of(3), nCopies(9999, 123),
                        nCopies(1_000_000, null))),
                200_000));

        tester.assertRoundTrip(INTEGER, values, false);

        tester.assertRoundTrip(
                VARCHAR,
                newArrayList(values).stream()
                        .map(value -> value == null ? null : String.valueOf(value))
                        .collect(toList()));
    }

    @Test
    public void testDwrfInvalidCheckpointsForStripeDictionary()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 200_000)).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testExtraLargeColumnValue()
            throws Exception
    {
        Random rnd = new Random(0);
        String val = rnd.doubles(50_000).mapToObj(Double::toString).collect(Collectors.joining(","));
        tester.assertRoundTrip(
                VARCHAR,
                ImmutableList.of(val));
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

    private static List<Float> floatSequence(float start, float step, int items)
    {
        Builder<Float> values = ImmutableList.builder();
        float nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values.build();
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

    public static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }
}
