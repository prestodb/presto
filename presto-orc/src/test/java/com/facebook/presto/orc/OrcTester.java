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
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.orc.OrcTester.Compression.NONE;
import static com.facebook.presto.orc.OrcTester.Compression.ZLIB;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Functions.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class OrcTester
{
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("Asia/Katmandu");

    private static final TypeManager TYPE_MANAGER = new TypeRegistry();

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

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, Type type)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, readValues, type);

        // all nulls
        assertRoundTrip(objectInspector, transform(readValues, constant(null)), type);

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(createHiveStructInspector(objectInspector),
                    transform(readValues, OrcTester::toHiveStruct),
                    rowType(type, type, type));
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(createHiveListInspector(objectInspector),
                    transform(readValues, OrcTester::toHiveList),
                    arrayType(type));
        }
    }

    private void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type rowType = rowType(elementType, elementType, elementType);
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector),
                transform(readValues, OrcTester::toHiveStruct),
                rowType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveStruct),
                    rowType);

            // all null values in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(transform(readValues, constant(null)), OrcTester::toHiveStruct),
                    rowType);
        }
    }

    private void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type mapType = mapType(elementType, elementType);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(readValues, value -> toHiveMap(value, readNullKeyValue)),
                mapType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);

            // all null values in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(transform(readValues, constant(null)), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);
        }
    }

    private void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, Type elementType)
            throws Exception
    {
        Type arrayType = arrayType(elementType);
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector),
                transform(readValues, OrcTester::toHiveList),
                arrayType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveList),
                    arrayType);

            // all null values in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(transform(readValues, constant(null)), OrcTester::toHiveList),
                    arrayType);
        }
    }

    private void testRoundTripType(ObjectInspector objectInspector, Iterable<?> readValues, Type type)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, readValues, type);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(objectInspector, reverse(readValues), type);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(objectInspector, insertNullEvery(5, readValues), type);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(objectInspector, insertNullEvery(5, reverse(readValues)), type);
            }
        }
    }

    public void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, Type type)
            throws Exception
    {
        for (Format formatVersion : formats) {
            MetadataReader metadataReader;
            if (DWRF == formatVersion) {
                if (hasType(objectInspector, PrimitiveCategory.DATE)) {
                    // DWRF doesn't support dates
                    return;
                }
                if (hasType(objectInspector, PrimitiveCategory.DECIMAL)) {
                    // DWRF doesn't support decimals
                    return;
                }
                metadataReader = new DwrfMetadataReader();
            }
            else {
                metadataReader = new OrcMetadataReader();
            }
            for (Compression compression : compressions) {
                try (TempFile tempFile = new TempFile()) {
                    writeOrcColumn(tempFile.getFile(), formatVersion, compression, objectInspector, readValues.iterator());

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
        assertEquals(recordReader.getReaderPosition(), 0);
        assertEquals(recordReader.getFilePosition(), 0);

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
                Block block = recordReader.readBlock(type, 0);

                List<Object> data = new ArrayList<>(block.getPositionCount());
                for (int position = 0; position < block.getPositionCount(); position++) {
                    data.add(type.getObjectValue(SESSION, block, position));
                }

                for (int i = 0; i < batchSize; i++) {
                    assertTrue(iterator.hasNext());
                    Object expected = iterator.next();
                    Object actual = data.get(i);
                    assertColumnValueEquals(type, actual, expected);
                }
            }
            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
            rowsProcessed += batchSize;
        }
        assertFalse(iterator.hasNext());

        assertEquals(recordReader.getReaderPosition(), rowsProcessed);
        assertEquals(recordReader.getFilePosition(), rowsProcessed);
        recordReader.close();
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        String baseType = type.getTypeSignature().getBase();
        if (ARRAY.equals(baseType)) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray.size(), expectedArray.size());

            Type elementType = type.getTypeParameters().get(0);
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (MAP.equals(baseType)) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            for (Entry<?, ?> actualEntry : actualMap.entrySet()) {
                Iterator<Entry<?, ?>> iterator = expectedEntries.iterator();
                while (iterator.hasNext()) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, actualEntry.getKey(), expectedEntry.getKey());
                        assertColumnValueEquals(valueType, actualEntry.getValue(), expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                    }
                }
            }
            assertTrue(expectedEntries.isEmpty(), "Unmatched entries " + expectedEntries);
        }
        else if (ROW.equals(baseType)) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.size());
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type.equals(DOUBLE)) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            if (actualDouble.isNaN()) {
                assertTrue(expectedDouble.isNaN(), "expected double to be NaN");
            }
            else {
                assertEquals(actualDouble, expectedDouble, 0.001);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(actual, expected);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, MetadataReader metadataReader, OrcPredicate predicate, Type type)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createRecordReader(ImmutableMap.of(0, type), predicate, HIVE_STORAGE_TIME_ZONE, new AggregatedMemoryContext());
    }

    static DataSize writeOrcColumn(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter;
        if (DWRF == format) {
            recordWriter = createDwrfRecordWriter(outputFile, compression, columnObjectInspector);
        }
        else {
            recordWriter = createOrcRecordWriter(outputFile, format, compression, columnObjectInspector);
        }
        return writeOrcFileColumnOld(outputFile, format, recordWriter, columnObjectInspector, values);
    }

    static DataSize writeOrcColumn(File outputFile, Format format, RecordWriter recordWriter, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
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

    public static DataSize writeOrcFileColumnOld(File outputFile, Format format, RecordWriter recordWriter, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        int i = 0;
        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueOld(getTypeInfoFromTypeString(columnObjectInspector.getTypeName()), value);
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

    private static Object preprocessWriteValueOld(TypeInfo typeInfo, Object value)
    {
        if (value == null) {
            return null;
        }
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return value;
                    case BYTE:
                        return ((Number) value).byteValue();
                    case SHORT:
                        return ((Number) value).shortValue();
                    case INT:
                        return ((Number) value).intValue();
                    case LONG:
                        return ((Number) value).longValue();
                    case FLOAT:
                        return ((Number) value).floatValue();
                    case DOUBLE:
                        return ((Number) value).doubleValue();
                    case DECIMAL:
                        return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
                    case STRING:
                        return value;
                    case DATE:
                        int days = ((SqlDate) value).getDays();
                        LocalDate localDate = LocalDate.ofEpochDay(days);
                        ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());

                        long millis = zonedDateTime.toEpochSecond() * 1000;
                        Date date = new Date(0);
                        // mills must be set separately to avoid masking
                        date.setTime(millis);
                        return date;
                    case TIMESTAMP:
                        long millisUtc = (int) ((SqlTimestamp) value).getMillisUtc();
                        return new Timestamp(millisUtc);
                    case BINARY:
                        return ((SqlVarbinary) value).getBytes();
                }
                break;
            case MAP:
                MapTypeInfo mapTypeInfo = checkType(typeInfo, MapTypeInfo.class, "fieldInspector");
                TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
                Map<Object, Object> newMap = new HashMap<>();
                for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    newMap.put(preprocessWriteValueOld(keyTypeInfo, entry.getKey()), preprocessWriteValueOld(valueTypeInfo, entry.getValue()));
                }
                return newMap;
            case LIST:
                ListTypeInfo listTypeInfo = checkType(typeInfo, ListTypeInfo.class, "fieldInspector");
                TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
                return ((List<?>) value).stream()
                        .map(element -> preprocessWriteValueOld(elementTypeInfo, element))
                        .collect(toList());
            case STRUCT:
                StructTypeInfo structTypeInfo = checkType(typeInfo, StructTypeInfo.class, "fieldInspector");
                List<?> fieldValues = (List<?>) value;
                List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                List<Object> newStruct = new ArrayList<>();
                for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                    newStruct.add(preprocessWriteValueOld(fieldTypeInfos.get(fieldId), fieldValues.get(fieldId)));
                }
                return newStruct;
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", typeInfo));
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

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
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

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
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

    static class TempFile
            implements Closeable
    {
        private final File tempDir;
        private final File file;

        public TempFile()
        {
            tempDir = createTempDir();
            tempDir.mkdirs();
            file = new File(tempDir, "data.rcfile");
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            // hadoop creates crc files that must be deleted also, so just delete the whole directory
            deleteRecursively(tempDir);
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
        return TYPE_MANAGER.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    private static Type mapType(Type keyType, Type valueType)
    {
        return TYPE_MANAGER.getParameterizedType(MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    private static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String filedName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(filedName, fieldType.getTypeSignature())));
        }
        return TYPE_MANAGER.getParameterizedType(ROW, typeSignatureParameters.build());
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value),
                "%s must be of type %s, not %s",
                name,
                target.getName(),
                value.getClass().getName());
        return target.cast(value);
    }
}
