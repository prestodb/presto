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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> transform, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, transform), parameterType);
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type parameterType)
            throws Exception
    {
        testRoundTrip(objectInspector, writeValues, readValues, readValues, parameterType);
    }

    public void testRoundTrip(
            ObjectInspector objectInspector,
            Iterable<?> writeValues,
            Iterable<?> readValues,
            Iterable<?> readStackValues,
            Type parameterType)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, writeValues, readValues);

        // all nulls
        assertRoundTrip(objectInspector, transform(writeValues, constant(null)), transform(readValues, constant(null)));

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(objectInspector, writeValues, readStackValues, parameterType);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(createHiveStructInspector(objectInspector),
                    transform(writeValues, OrcTester::toHiveStruct),
                    transform(readStackValues, OrcTester::toObjectStruct),
                    parameterType);
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(objectInspector, writeValues, readStackValues, parameterType);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(objectInspector, writeValues, readStackValues, parameterType);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(createHiveListInspector(objectInspector),
                    transform(writeValues, OrcTester::toHiveList),
                    transform(readStackValues, OrcTester::toObjectList),
                    parameterType);
        }
    }

    private void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type parameterType)
            throws Exception
    {
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector),
                transform(writeValues, OrcTester::toHiveStruct),
                transform(readValues, value -> toBlockStruct(value, parameterType)));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveStruct),
                    transform(insertNullEvery(5, readValues), value -> toBlockStruct(value, parameterType)));

            // all null values in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveStruct),
                    transform(transform(writeValues, constant(null)), value -> toBlockStruct(value, parameterType)));
        }
    }

    private void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type parameterType)
            throws Exception
    {
        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(writeValues, OrcTester::toHiveMap),
                transform(readValues, value -> toBlockMap(value, parameterType)));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveMap),
                    transform(insertNullEvery(5, readValues), value -> toBlockMap(value, parameterType)));

            // all null values in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveMap),
                    transform(transform(readValues, constant(null)), value -> toBlockMap(value, parameterType)));
        }
    }

    private void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type parameterType)
            throws Exception
    {
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector),
                transform(writeValues, OrcTester::toHiveList),
                transform(readValues, value -> toBlockList(value, parameterType)));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(insertNullEvery(5, writeValues), OrcTester::toHiveList),
                    transform(insertNullEvery(5, readValues), value -> toBlockList(value, parameterType)));

            // all null values in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(transform(writeValues, constant(null)), OrcTester::toHiveList),
                    transform(transform(readValues, constant(null)), value -> toBlockList(value, parameterType)));
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

    public void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues)
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
                    if (expected instanceof String) {
                        expected = Slices.utf8Slice((String) expected);
                    }

                    if (!Objects.equals(objectVector.vector[i], expected)) {
                        assertEquals(objectVector.vector[i], expected);
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

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, MetadataReader metadataReader, OrcPredicate predicate)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));

        return orcReader.createRecordReader(ImmutableSet.of(0), predicate, HIVE_STORAGE_TIME_ZONE);
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
                () -> {}
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
                () -> {}
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
        return getStandardStructObjectInspector(ImmutableList.of("a", "b"), ImmutableList.of(objectInspector, objectInspector));
    }

    private static Object toHiveStruct(Object input)
    {
        return new Object[] {input, input};
    }

    private static Object toBlockStruct(Object input, Type parameterType)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        appendToBlockBuilder(parameterType, input, blockBuilder);
        appendToBlockBuilder(parameterType, input, blockBuilder);
        return buildStructuralSlice(blockBuilder);
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

    private static Object toHiveMap(Object input)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input, input);
        return map;
    }

    private static Object toBlockMap(Object input, Type parameterType)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        appendToBlockBuilder(parameterType, input, blockBuilder);
        appendToBlockBuilder(parameterType, input, blockBuilder);
        return buildStructuralSlice(blockBuilder);
    }

    private static StandardListObjectInspector createHiveListInspector(ObjectInspector objectInspector)
    {
        return getStandardListObjectInspector(objectInspector);
    }

    private static Object toHiveList(Object input)
    {
        List<Object> list = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            list.add(input);
        }
        return list;
    }

    private static Object toBlockList(Object input, Type parameterType)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (int i = 0; i < 4; i++) {
            appendToBlockBuilder(parameterType, input, blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    private static Slice buildStructuralSlice(BlockBuilder builder)
    {
        BlockEncoding encoding = builder.getEncoding();
        Block block = builder.build();
        DynamicSliceOutput output = new DynamicSliceOutput(encoding.getEstimatedSize(block));
        encoding.writeBlock(output, block);

        return output.slice();
    }

    private static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, ((Number) element).longValue());
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) element);
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("Unexpected type %s", javaType.getName()));
        }
    }

    private static Object toObjectList(Object input)
    {
        if (input instanceof Float) {
            input = ((Float) input).doubleValue();
        }
        List<Object> list = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            list.add(input);
        }
        return list;
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
}
