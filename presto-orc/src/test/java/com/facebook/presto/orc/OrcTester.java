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

import com.facebook.hive.orc.OrcConf;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.orc.OrcTester.Compression.NONE;
import static com.facebook.presto.orc.OrcTester.Compression.ZLIB;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.orc.Vector.MAX_VECTOR_LENGTH;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class OrcTester
{
    private static final JsonCodec<Object> OBJECT_JSON_CODEC = new JsonCodecFactory().jsonCodec(Object.class);
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("Asia/Katmandu");

    public enum Format
    {
        ORC_12, ORC_11, DWRF
    }

    public enum Compression
    {
        ZLIB, SNAPPY, NONE
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean structuralNullTestsEnabled;
    private boolean reverseTestsEnabled;
    private boolean nullTestsEnabled;
    private boolean skipBatchTestsEnabled;
    private boolean skipStripeTestsEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<Compression> compressions = ImmutableSet.of();

    public static OrcTester quickOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12, DWRF);
        orcTester.compressions = ImmutableSet.of(ZLIB);
        return orcTester;
    }

    public static OrcTester fullOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.complexStructuralTestsEnabled = true;
        orcTester.structuralNullTestsEnabled = true;
        orcTester.reverseTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.skipStripeTestsEnabled = true;
        orcTester.formats = ImmutableSet.copyOf(Format.values());
        orcTester.compressions = ImmutableSet.copyOf(Compression.values());
        return orcTester;
    }

    public void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues);
    }

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> transform)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, transform));
    }

    public <W, R, J> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector,
            Iterable<W> writeValues,
            Function<W, R> readTransform,
            Function<W, J> readJsonTransform)
            throws Exception
    {
        Iterable<R> readValues = transform(writeValues, readTransform);
        Iterable<J> readJsonJsonValues = transform(writeValues, readJsonTransform);
        testRoundTrip(columnObjectInspector, writeValues, readValues, readValues);
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        testRoundTrip(objectInspector, writeValues, readValues, readValues);
    }

    public void testRoundTrip(
            ObjectInspector objectInspector,
            Iterable<?> writeValues,
            Iterable<?> readValues,
            Iterable<?> readJsonStackValues)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, writeValues, readValues);

        // all nulls
        assertRoundTrip(objectInspector, transform(writeValues, constant(null)), transform(readValues, constant(null)));

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(objectInspector, writeValues, readJsonStackValues);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(createHiveStructInspector(objectInspector), transform(writeValues, OrcTester::toHiveStruct), transform(readJsonStackValues, OrcTester::toObjectStruct));
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(objectInspector, writeValues, readJsonStackValues);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(objectInspector, writeValues, readJsonStackValues);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(createHiveListInspector(objectInspector), transform(writeValues, OrcTester::toHiveList), transform(readJsonStackValues, OrcTester::toObjectList));
        }
    }

    private void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector), transform(writeValues, OrcTester::toHiveStruct), transform(readJsonValues, OrcTester::toJsonStruct));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveStruct),
                    transform(insertNullEvery(5, readJsonValues), OrcTester::toJsonStruct));

            // all null values in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveStruct),
                    transform(transform(writeValues, constant(null)), OrcTester::toJsonStruct));
        }
    }

    private void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // json does not support null keys, so we just write the first value
        Object nullKeyWrite = writeValues.iterator().next();
        Object nullKeyRead = readJsonValues.iterator().next();

        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(writeValues, value -> toHiveMap(nullKeyWrite, value)),
                transform(readJsonValues, value -> toJsonMap(nullKeyRead, value)));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), value -> toHiveMap(nullKeyWrite, value)),
                    transform(insertNullEvery(5, readJsonValues), value -> toJsonMap(nullKeyRead, value)));

            // all null values in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), value -> toHiveMap(nullKeyWrite, value)),
                    transform(transform(readJsonValues, constant(null)), value -> toJsonMap(nullKeyRead, value)));
        }
    }

    private void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readJsonValues)
            throws Exception
    {
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector), transform(writeValues, OrcTester::toHiveList), transform(readJsonValues, OrcTester::toJsonList));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveList),
                    transform(insertNullEvery(5, readJsonValues), OrcTester::toJsonList));

            // all null values in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveList),
                    transform(transform(readJsonValues, constant(null)), OrcTester::toJsonList));
        }
    }

    private void testRoundTripType(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, writeValues, readValues);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(objectInspector, reverse(writeValues), reverse(readValues));
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(objectInspector, insertNullEvery(5, writeValues), insertNullEvery(5, readValues));

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(objectInspector, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)));
            }
        }
    }

    private void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
            throws Exception
    {
        for (Format formatVersion : formats) {
            MetadataReader metadataReader;
            if (DWRF == formatVersion) {
                if (hasType(objectInspector, PrimitiveCategory.DATE)) {
                    // DWRF doesn't support dates
                    return;
                }
                metadataReader = new DwrfMetadataReader();
            }
            else {
                metadataReader = new OrcMetadataReader();
            }
            for (Compression compression : compressions) {
                try (TempFile tempFile = new TempFile("test", "orc")) {
                    writeOrcColumn(tempFile.getFile(), formatVersion, compression, objectInspector, writeValues.iterator());

                    assertFileContents(objectInspector, tempFile, readValues, false, false, metadataReader);

                    if (skipBatchTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, true, false, metadataReader);
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, false, true, metadataReader);
                    }
                }
            }
        }
    }

    private static void assertFileContents(ObjectInspector objectInspector,
            TempFile tempFile,
            Iterable<?> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe,
            MetadataReader metadataReader)
            throws IOException
    {
        OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, metadataReader, createOrcPredicate(objectInspector, expectedValues));

        Vector vector = createResultsVector(objectInspector);

        boolean isFirst = true;
        int rowsProcessed = 0;
        Iterator<?> iterator = expectedValues.iterator();
        for (int batchSize = Ints.checkedCast(recordReader.nextBatch()); batchSize >= 0; batchSize = Ints.checkedCast(recordReader.nextBatch())) {
            if (skipStripe && rowsProcessed < 10000) {
                assertEquals(advance(iterator, batchSize), batchSize);
            }
            else if (skipFirstBatch && isFirst) {
                assertEquals(advance(iterator, batchSize), batchSize);
                isFirst = false;
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
        }
        assertFalse(iterator.hasNext());
        recordReader.close();
    }

    private static Vector createResultsVector(ObjectInspector objectInspector)
    {
        if (!(objectInspector instanceof PrimitiveObjectInspector)) {
            return new SliceVector(MAX_VECTOR_LENGTH);
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
        PrimitiveCategory primitiveCategory = primitiveObjectInspector.getPrimitiveCategory();

        switch (primitiveCategory) {
            case BOOLEAN:
                return new BooleanVector(MAX_VECTOR_LENGTH);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
            case TIMESTAMP:
                return new LongVector(MAX_VECTOR_LENGTH);
            case FLOAT:
            case DOUBLE:
                return new DoubleVector(MAX_VECTOR_LENGTH);
            case BINARY:
            case STRING:
                return new SliceVector(MAX_VECTOR_LENGTH);
            default:
                throw new IllegalArgumentException("Unsupported types " + primitiveCategory);
        }
    }

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, MetadataReader metadataReader, OrcPredicate predicate)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));

        return orcReader.createRecordReader(ImmutableSet.of(0), predicate, HIVE_STORAGE_TIME_ZONE);
    }

    public static DataSize writeOrcColumn(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter;
        if (DWRF == format) {
            recordWriter = createDwrfRecordWriter(outputFile, compression, columnObjectInspector);
        }
        else {
            recordWriter = createOrcRecordWriter(outputFile, format, compression, columnObjectInspector);
        }

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        while (values.hasNext()) {
            Object value = values.next();
            objectInspector.setStructFieldData(row, fields.get(0), value);

            @SuppressWarnings("deprecation") Serializer serde;
            if (DWRF == format) {
                serde = new com.facebook.hive.orc.OrcSerde();
            }
            else {
                serde = new OrcSerde();
            }
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    public static RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );
    }

    public static RecordWriter createDwrfRecordWriter(File outputFile, Compression compressionCodec, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.default.compress", compressionCodec.name());
        jobConf.set("hive.exec.orc.compress", compressionCodec.name());
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD, 1);
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_ENCODING_INTERVAL, 2);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_BUILD_STRIDE_DICTIONARY, true);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS, true);
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        return new com.facebook.hive.orc.OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != NONE,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );
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

    private static <T> Iterable<T> reverse(Iterable<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
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

    private static StandardStructObjectInspector createHiveStructInspector(ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of("a", "b"), ImmutableList.of(objectInspector, objectInspector));
    }

    private static Object toHiveStruct(Object input)
    {
        return new Object[] {input, input};
    }

    private static Object toJsonStruct(Object input)
    {
        if (input instanceof Float) {
            input = ((Float) input).doubleValue();
        }
        List<Object> data = new ArrayList<>();
        data.add(input);
        data.add(input);
        return OBJECT_JSON_CODEC.toJson(data);
    }

    private static Object toObjectStruct(Object input)
    {
        if (input instanceof Float) {
            input = ((Float) input).doubleValue();
        }
        List<Object> data = new ArrayList<>();
        data.add(input);
        data.add(input);
        return data;
    }

    private static StandardMapObjectInspector createHiveMapInspector(ObjectInspector objectInspector)
    {
        return getStandardMapObjectInspector(objectInspector, objectInspector);
    }

    private static Object toHiveMap(final Object nullKeyValue, Object input)
    {
        Map<Object, Object> map = new HashMap<>();
        if (input == null) {
            // json doesn't support null keys, so just write the nullKeyValue
            map.put(nullKeyValue, null);
        }
        else {
            map.put(input, input);
        }
        return map;
    }

    private static Object toJsonMap(final Object nullKeyValue, Object input)
    {
        Map<Object, Object> map = new HashMap<>();
        if (input == null) {
            // json doesn't support null keys, so just write the nullKeyValue
            map.put(nullKeyValue, null);
        }
        else {
            map.put(input, input);
        }
        return OBJECT_JSON_CODEC.toJson(map);
    }

    private static StandardListObjectInspector createHiveListInspector(ObjectInspector objectInspector)
    {
        return getStandardListObjectInspector(objectInspector);
    }

    private static Object toHiveList(Object input)
    {
        ArrayList<Object> list = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            list.add(input);
        }
        return list;
    }

    private static Object toJsonList(Object input)
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

    private static Object toObjectList(Object input)
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

    public static boolean hasType(ObjectInspector objectInspector, PrimitiveCategory... types)
    {
        if (objectInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) objectInspector;
            PrimitiveCategory primitiveCategory = primitiveInspector.getPrimitiveCategory();
            for (PrimitiveCategory type : types) {
                if (primitiveCategory == type) {
                    return true;
                }
            }
            return false;
        }
        if (objectInspector instanceof ListObjectInspector) {
            ListObjectInspector listInspector = (ListObjectInspector) objectInspector;
            return hasType(listInspector.getListElementObjectInspector(), types);
        }
        if (objectInspector instanceof MapObjectInspector) {
            MapObjectInspector mapInspector = (MapObjectInspector) objectInspector;
            return hasType(mapInspector.getMapKeyObjectInspector(), types) ||
                    hasType(mapInspector.getMapValueObjectInspector(), types);
        }
        if (objectInspector instanceof StructObjectInspector) {
            for (StructField field : ((StructObjectInspector) objectInspector).getAllStructFieldRefs()) {
                if (hasType(field.getFieldObjectInspector(), types)) {
                    return true;
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unknown object inspector type " + objectInspector);
    }
}
