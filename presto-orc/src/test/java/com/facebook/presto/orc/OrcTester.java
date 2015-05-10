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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
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
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class OrcTester
{
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
        orcTester.formats = ImmutableSet.of(ORC_12);
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

    public void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues, parameterType);
    }

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> readTransform, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, readTransform), parameterType);
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, writeValues, readValues, type);

        // all nulls
        assertRoundTrip(objectInspector, transform(writeValues, constant(null)), transform(readValues, constant(null)), type);

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(objectInspector, writeValues, readValues, type);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(createHiveStructInspector(objectInspector),
                    transform(writeValues, OrcTester::toHiveStruct),
                    transform(readValues, OrcTester::toHiveStruct),
                    rowType(type, type, type));
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(objectInspector, writeValues, readValues, type);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(objectInspector, writeValues, readValues, type);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(createHiveListInspector(objectInspector),
                    transform(writeValues, OrcTester::toHiveList),
                    transform(readValues, OrcTester::toHiveList),
                    arrayType(type));
        }
    }

    private void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type rowType = rowType(elementType, elementType, elementType);
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector),
                transform(writeValues, OrcTester::toHiveStruct),
                transform(readValues, OrcTester::toHiveStruct),
                rowType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveStruct),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveStruct),
                    rowType);

            // all null values in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveStruct),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveStruct),
                    rowType);
        }
    }

    private void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type mapType = mapType(elementType, elementType);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object writeNullKeyValue = Iterables.getLast(writeValues);
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(writeValues, value -> toHiveMap(value, writeNullKeyValue)),
                transform(readValues, value -> toHiveMap(value, readNullKeyValue)),
                mapType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), value -> toHiveMap(value, writeNullKeyValue)),
                    transform(insertNullEvery(5, readValues), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);

            // all null values in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), value -> toHiveMap(value, writeNullKeyValue)),
                    transform(transform(readValues, constant(null)), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);
        }
    }

    private void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type arrayType = arrayType(elementType);
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector),
                transform(writeValues, OrcTester::toHiveList),
                transform(readValues, OrcTester::toHiveList),
                arrayType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveList),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveList),
                    arrayType);

            // all null values in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveList),
                    transform(transform(readValues, constant(null)), OrcTester::toHiveList),
                    arrayType);
        }
    }

    private void testRoundTripType(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, writeValues, readValues, type);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(objectInspector, reverse(writeValues), reverse(readValues), type);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(objectInspector, insertNullEvery(5, writeValues), insertNullEvery(5, readValues), type);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(objectInspector, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)), type);
            }
        }
    }

    public void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
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

                    assertFileContents(objectInspector, tempFile, readValues, false, false, metadataReader, type);

                    if (skipBatchTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, true, false, metadataReader, type);
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, false, true, metadataReader, type);
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
            MetadataReader metadataReader,
            Type type)
            throws IOException
    {
        OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, metadataReader, createOrcPredicate(objectInspector, expectedValues), type);

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

                    Object actual = objectVector.vector[i];
                    if (actual instanceof Slice) {
                        actual = decodeSlice(type, (Slice) actual);
                    }

                    if (!Objects.equals(actual, expected)) {
                        assertEquals(actual, expected);
                    }
                }
            }
            rowsProcessed += batchSize;
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

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, MetadataReader metadataReader, OrcPredicate predicate, Type type)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));

        return orcReader.createRecordReader(ImmutableMap.of(0, type), predicate, HIVE_STORAGE_TIME_ZONE);
    }

    private static DataSize writeOrcColumn(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector, Iterator<?> values)
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

        int i = 0;
        while (values.hasNext()) {
            Object value = values.next();
            objectInspector.setStructFieldData(row, fields.get(0), value);

            @SuppressWarnings("deprecation") Serializer serde;
            if (DWRF == format) {
                serde = new com.facebook.hive.orc.OrcSerde();
                if (i == 142_345) {
                    setDwrfLowMemoryFlag(recordWriter);
                }
            }
            else {
                serde = new OrcSerde();
            }
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
            i++;
        }

        recordWriter.close(false);
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    private static void setDwrfLowMemoryFlag(RecordWriter recordWriter)
    {
        Object writer = getFieldValue(recordWriter, "writer");
        Object memoryManager = getFieldValue(writer, "memoryManager");
        setFieldValue(memoryManager, "lowMemoryMode", true);
        try {
            writer.getClass().getMethod("enterLowMemoryMode").invoke(writer);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static Object getFieldValue(Object instance, String name)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            return writerField.get(instance);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static void setFieldValue(Object instance, String name, Object value)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            writerField.set(instance, value);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
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
                () -> { }
        );
    }

    private static RecordWriter createDwrfRecordWriter(File outputFile, Compression compressionCodec, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.default.compress", compressionCodec.name());
        jobConf.set("hive.exec.orc.compress", compressionCodec.name());
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD, 1);
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_ENCODING_INTERVAL, 2);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_BUILD_STRIDE_DICTIONARY, true);
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        return new com.facebook.hive.orc.OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != NONE,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                () -> { }
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

    private static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
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

    private static StandardStructObjectInspector createHiveStructInspector(ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of("a", "b", "c"), ImmutableList.of(objectInspector, objectInspector, objectInspector));
    }

    private static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static StandardMapObjectInspector createHiveMapInspector(ObjectInspector objectInspector)
    {
        return getStandardMapObjectInspector(objectInspector, objectInspector);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static StandardListObjectInspector createHiveListInspector(ObjectInspector objectInspector)
    {
        return getStandardListObjectInspector(objectInspector);
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static Object decodeSlice(Type type, Slice slice)
    {
        String base = type.getTypeSignature().getBase();
        if (base.equals(ARRAY)) {
            Block block = new VariableWidthBlockEncoding().readBlock(slice.getInput());

            Type elementType = type.getTypeParameters().get(0);

            List<Object> array = new ArrayList<>();
            for (int position = 0; position < block.getPositionCount(); position++) {
                array.add(getBlockValue(block, position, elementType));
            }
            return array;
        }
        else if (base.equals(ROW)) {
            Block block = new VariableWidthBlockEncoding().readBlock(slice.getInput());

            List<Type> fieldTypes = type.getTypeParameters();

            List<Object> row = new ArrayList<>();
            for (int field = 0; field < block.getPositionCount(); field++) {
                row.add(getBlockValue(block, field, fieldTypes.get(field)));
            }
            return row;
        }
        else if (base.equals(MAP)) {
            Block block = new VariableWidthBlockEncoding().readBlock(slice.getInput());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            Map<Object, Object> map = new LinkedHashMap<>();
            int entryCount = block.getPositionCount() / 2;
            for (int entry = 0; entry < entryCount; entry++) {
                int blockPosition = entry * 2;
                Object key = getBlockValue(block, blockPosition, keyType);
                // null keys are not allowed
                if (key != null) {
                    Object value = getBlockValue(block, blockPosition + 1, valueType);
                    map.put(key, value);
                }
            }
            return map;
        }
        if (type.equals(VARCHAR) || type.equals(VARBINARY)) {
            return slice.toStringUtf8();
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static Object getBlockValue(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            return null;
        }
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        if (javaType == Slice.class) {
            return decodeSlice(type, type.getSlice(block, position));
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static boolean hasType(ObjectInspector objectInspector, PrimitiveCategory... types)
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

    private static Type arrayType(Type elementType)
    {
        return new MockStructuralType(ARRAY, ImmutableList.of(elementType));
    }

    private static Type mapType(Type keyType, Type valueType)
    {
        return new MockStructuralType(MAP, ImmutableList.of(keyType, valueType));
    }

    private static Type rowType(Type... fieldTypes)
    {
        return new MockStructuralType(ROW, ImmutableList.copyOf(fieldTypes));
    }

    private static class MockStructuralType
            extends AbstractVariableWidthType
    {
        private final List<Type> types;

        public MockStructuralType(String base, List<Type> types)
        {
            super(new TypeSignature(base, ImmutableList.copyOf(transform(types, Type::getTypeSignature)), ImmutableList.of()), Slice.class);
            this.types = types;
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            return null;
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypeParameters()
        {
            return ImmutableList.copyOf(types);
        }

        @Override
        public Slice getSlice(Block block, int position)
        {
            return block.getSlice(position, 0, block.getLength(position));
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value)
        {
            blockBuilder.writeBytes(value, 0, value.length()).closeEntry();
        }
    }
}
