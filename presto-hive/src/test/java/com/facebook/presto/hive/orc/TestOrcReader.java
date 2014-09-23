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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.google.common.base.Functions.compose;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BYTE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.FLOAT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.LONG;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.SHORT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcReader
{
    private static final JsonCodec<Object> OBJECT_JSON_CODEC = new JsonCodecFactory().jsonCodec(Object.class);

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        testRoundTrip(javaBooleanObjectInspector, limit(cycle(ImmutableList.of(true, false, false)), 30_000));
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

    private static void testRoundTripNumeric(Iterable<Integer> writeValues)
            throws Exception
    {
        testRoundTrip(javaByteObjectInspector, transform(writeValues, intToByte()), byteToLong());
        testRoundTrip(javaShortObjectInspector, transform(writeValues, intToShort()), shortToLong());
        testRoundTrip(javaIntObjectInspector, writeValues, intToLong());
        testRoundTrip(javaLongObjectInspector, transform(writeValues, intToLong()));
        testRoundTrip(javaTimestampObjectInspector, transform(writeValues, intToTimestamp()), timestampToLong(), timestampToString());
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        testRoundTrip(javaFloatObjectInspector, floatSequence(0.0f, 0.1f, 30_000), floatToDouble());
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        testRoundTrip(javaStringObjectInspector, transform(intsBetween(0, 30_000), toStringFunction()));
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        testRoundTrip(javaStringObjectInspector, limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), toStringFunction())), 30_000));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        testRoundTrip(javaStringObjectInspector, limit(cycle(""), 30_000));
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        testRoundTrip(javaByteArrayObjectInspector,
                transform(intsBetween(0, 30_000), compose(stringToByteArray(), toStringFunction())),
                byteArrayToString(),
                byteArrayToBase64());
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        testRoundTrip(javaByteArrayObjectInspector,
                limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), compose(stringToByteArray(), toStringFunction()))), 30_000),
                byteArrayToString(),
                byteArrayToBase64());
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        testRoundTrip(javaByteArrayObjectInspector, limit(cycle(new byte[0]), 30_000), byteArrayToString());
    }

    private static void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues);
    }

    private static <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> transform)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, transform));
    }

    private static <W, R, J> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector,
            Iterable<W> writeValues,
            Function<W, R> readTransform,
            Function<W, J> readJsonTransform)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, readTransform), transform(writeValues, readJsonTransform));
    }

    private static void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        testRoundTrip(objectInspector, writeValues, readValues, readValues);
    }

    private static void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, writeValues, readValues);

        // values wrapped in struct
        testStructRoundTrip(objectInspector, writeValues, readJsonValues);

        // values wrapped in a struct wrapped in a struct
        testStructRoundTrip(createHiveStructInspector(objectInspector), transform(writeValues, toHiveStruct()), transform(readJsonValues, toObjectStruct()));

        // values wrapped in map
        testMapRoundTrip(objectInspector, writeValues, readJsonValues);

        // values wrapped in list
        testListRoundTrip(objectInspector, writeValues, readJsonValues);

        // values wrapped in a list wrapped in a list
        testListRoundTrip(createHiveListInspector(objectInspector), transform(writeValues, toHiveList()), transform(readJsonValues, toObjectList()));
    }

    private static void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector), transform(writeValues, toHiveStruct()), transform(readJsonValues, toJsonStruct()));

        // values and nulls in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector),
                transform(insertNullEvery(5, writeValues), toHiveStruct()),
                transform(insertNullEvery(5, readJsonValues), toJsonStruct()));
    }

    private static void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector), transform(writeValues, toHiveMap()), transform(readJsonValues, toJsonMap()));

        // values and nulls in simple map
        // json does not support null keys, so we just duplicate the previous entry instead of adding null
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(insertNullEvery(5, writeValues), toHiveMap()),
                transform(insertNullEvery(5, readJsonValues), toJsonMap()));
    }

    private static void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector), transform(writeValues, toHiveList()), transform(readJsonValues, toJsonList()));

        // values and nulls in simple list
        testRoundTripType(createHiveListInspector(objectInspector),
                transform(insertNullEvery(5, writeValues), toHiveList()),
                transform(insertNullEvery(5, readJsonValues), toJsonList()));
    }

    private static void testRoundTripType(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, writeValues, readValues);

        // reverse order
        assertRoundTrip(objectInspector, reverse(writeValues), reverse(readValues));

        // forward order with nulls
        assertRoundTrip(objectInspector, insertNullEvery(5, writeValues), insertNullEvery(5, readValues));

        // reverse order with nulls
        assertRoundTrip(objectInspector, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)));
    }

    private static void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        for (String formatVersion : ImmutableList.of("0.12"/*, "0.11"*/)) {
            for (String compressionKind : ImmutableList.of("ZLIB"/*, "SNAPPY", "NONE"*/)) {
                try (TempFile tempFile = new TempFile("test", "orc")) {
                    writeOrcColumn(tempFile.getFile(), formatVersion, compressionKind, objectInspector, writeValues.iterator());
                    assertFileContents(objectInspector, tempFile, readValues, false);
                    assertFileContents(objectInspector, tempFile, readValues, true);
                }
            }
        }
    }

    private static void assertFileContents(ObjectInspector objectInspector, TempFile tempFile, Iterable<?> expectedValues, boolean skipFirstBatch)
            throws IOException
    {
        OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, HiveType.getHiveType(objectInspector));

        Vector vector = createResultsVector(objectInspector);

        boolean isFirst = true;
        Iterator<?> iterator = expectedValues.iterator();
        for (int batchSize = Ints.checkedCast(recordReader.nextBatch()); batchSize >= 0; batchSize = Ints.checkedCast(recordReader.nextBatch())) {
            if (skipFirstBatch && isFirst) {
                assertEquals(advance(iterator, batchSize), batchSize);
            }
            else {
                recordReader.readVector(0, vector);

                ObjectVector objectVector = vector.toObjectVector(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    assertTrue(iterator.hasNext());
                    Object expected = iterator.next();

                    if (!Objects.equals(objectVector.vector[i], expected)) {
                        assertEquals(objectVector.vector[i], expected);
                    }
                }
            }
            isFirst = false;
        }
        assertFalse(iterator.hasNext());
        recordReader.close();
    }

    private static Vector createResultsVector(ObjectInspector objectInspector)
    {
        if (!(objectInspector instanceof PrimitiveObjectInspector)) {
            return new SliceVector();
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
        PrimitiveCategory primitiveCategory = primitiveObjectInspector.getPrimitiveCategory();

        if (primitiveCategory == BOOLEAN) {
            return new BooleanVector();
        }
        else if (primitiveCategory == BYTE || primitiveCategory == SHORT || primitiveCategory == INT || primitiveCategory == LONG || primitiveCategory == TIMESTAMP) {
            return new LongVector();
        }
        else if (primitiveCategory == FLOAT || primitiveCategory == DOUBLE) {
            return new DoubleVector();
        }
        else if (primitiveCategory == BINARY || primitiveCategory == STRING) {
            return new SliceVector();
        }
        else {
            throw new IllegalArgumentException("Unsupported types " + primitiveCategory);
        }
    }

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, HiveType hiveType)
            throws IOException
    {
        Path path = new Path(tempFile.getFile().toURI());
        FileSystem fileSystem = path.getFileSystem(new Configuration());

        OrcReader orcReader = new OrcReader(path, fileSystem);
        HiveColumnHandle columnHandle = new HiveColumnHandle("clientId", "col_0", 0, hiveType, 0, false);

        return orcReader.createRecordReader(0, tempFile.getFile().length(), ImmutableList.of(columnHandle), TupleDomain.<HiveColumnHandle>all(), UTC);
    }

    public static DataSize writeOrcColumn(File outputFile, String formatVersion, String compressionCodec, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", formatVersion);
        jobConf.set("hive.exec.orc.default.compress", compressionCodec);
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        RecordWriter recordWriter1 = new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != null,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );

        RecordWriter recordWriter = recordWriter1;

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        while (values.hasNext()) {
            Object value = values.next();
            objectInspector.setStructFieldData(row, fields.get(0), value);
            Writable record = new OrcSerde().serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    private static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(objectInspector));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        return orderTableProperties;
    }

    private static class TempFile
            implements Closeable
    {
        private final File file;

        private TempFile(String prefix, String suffix)
        {
            try {
                file = File.createTempFile(prefix, suffix);
                file.delete();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            file.delete();
        }
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

    private static <T> Iterable<T> insertNullEvery(final int n, final Iterable<T> iterable)
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
                        position++;
                        if (position > n) {
                            position = 0;
                            return null;
                        }

                        if (!delegate.hasNext()) {
                            return endOfData();
                        }

                        return delegate.next();
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

    private static Iterable<Float> floatSequence(final double start, final double step, final int items)
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

    private static <T> Iterable<T> reverse(Iterable<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
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

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();

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

    private static boolean[] findIncludedColumns(List<Type> types)
    {
        boolean[] includes = new boolean[types.size()];
        includes[0] = true;

        includeColumnRecursive(types, includes, 1);

        return includes;
    }

    private static void includeColumnRecursive(List<Type> types, boolean[] result, int typeId)
    {
        result[typeId] = true;
        Type type = types.get(typeId);
        int children = type.getSubtypesCount();
        for (int i = 0; i < children; ++i) {
            includeColumnRecursive(types, result, type.getSubtypes(i));
        }
    }

    private static StandardStructObjectInspector createHiveStructInspector(ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of("a", "b"), ImmutableList.of(objectInspector, objectInspector));
    }

    private static Function<Object, Object> toHiveStruct()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                return new Object[] {input, input};
            }
        };
    }

    private static Function<Object, Object> toJsonStruct()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                if (input instanceof Float) {
                    input = ((Float) input).doubleValue();
                }
                Map<Object, Object> data = new LinkedHashMap<>();
                data.put("a", input);
                data.put("b", input);
                return OBJECT_JSON_CODEC.toJson(data);
            }
        };
    }

    private static Function<Object, Object> toObjectStruct()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                if (input instanceof Float) {
                    input = ((Float) input).doubleValue();
                }
                Map<Object, Object> data = new LinkedHashMap<>();
                data.put("a", input);
                data.put("b", input);
                return data;
            }
        };
    }

    private static StandardMapObjectInspector createHiveMapInspector(ObjectInspector objectInspector)
    {
        return getStandardMapObjectInspector(objectInspector, objectInspector);
    }

    private static Function<Object, Object> toHiveMap()
    {
        return new Function<Object, Object>()
        {
            private Object previousInput;

            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                Map<Object, Object> map = new HashMap<>();
                if (input == null) {
                    // json doesn't support null keys, so just write the previous value as the key
                    map.put(previousInput, input);
                }
                else {
                    map.put(input, input);
                }
                previousInput = input;
                return map;
            }
        };
    }

    private static Function<Object, Object> toJsonMap()
    {
        return new Function<Object, Object>()
        {
            private Object previousInput;

            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                Map<Object, Object> map = new HashMap<>();
                if (input == null) {
                    // json doesn't support null keys, so just write the previous value as the key
                    map.put(previousInput, input);
                }
                else {
                    map.put(input, input);
                }
                previousInput = input;
                return OBJECT_JSON_CODEC.toJson(map);
            }
        };
    }

    private static Function<Object, Object> toObjectMap()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                if (input instanceof Float) {
                    input = ((Float) input).doubleValue();
                }
                Map<Object, Object> data = new LinkedHashMap<>();
                data.put(input, input);
                return data;
            }
        };
    }

    private static StandardListObjectInspector createHiveListInspector(ObjectInspector objectInspector)
    {
        return getStandardListObjectInspector(objectInspector);
    }

    private static Function<Object, Object> toHiveList()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                ArrayList<Object> list = new ArrayList<>(4);
                for (int i = 0; i < 4; i++) {
                    list.add(input);
                }
                return list;
            }
        };
    }

    private static Function<Object, Object> toJsonList()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                if (input instanceof Float) {
                    input = ((Float) input).doubleValue();
                }
                ArrayList<Object> list = new ArrayList<>(4);
                for (int i = 0; i < 4; i++) {
                    list.add(input);
                }
                return OBJECT_JSON_CODEC.toJson(list);
            }
        };
    }

    private static Function<Object, Object> toObjectList()
    {
        return new Function<Object, Object>()
        {
            @Nullable
            @Override
            public Object apply(@Nullable Object input)
            {
                if (input instanceof Float) {
                    input = ((Float) input).doubleValue();
                }
                ArrayList<Object> list = new ArrayList<>(4);
                for (int i = 0; i < 4; i++) {
                    list.add(input);
                }
                return list;
            }
        };
    }
}
