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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

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
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
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
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
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
        ORC_12 {
            @Override
            public MetadataReader createMetadataReader()
            {
                return new OrcMetadataReader();
            }

            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new OrcSerde();
            }
        },
        ORC_11 {
            @Override
            public MetadataReader createMetadataReader()
            {
                return new OrcMetadataReader();
            }

            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new OrcSerde();
            }
        },
        DWRF {
            @Override
            public MetadataReader createMetadataReader()
            {
                return new DwrfMetadataReader();
            }

            @Override
            public boolean supportsType(Type type)
            {
                return !hasType(type, ImmutableSet.of(DATE, DECIMAL, CHAR));
            }

            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new com.facebook.hive.orc.OrcSerde();
            }
        };

        public abstract MetadataReader createMetadataReader();

        public boolean supportsType(Type type)
        {
            return true;
        }

        @SuppressWarnings("deprecation")
        public abstract Serializer createSerializer();
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

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        // just the values
        testRoundTripType(type, readValues);

        // all nulls
        assertRoundTrip(
                type,
                readValues.stream()
                        .map(value -> null)
                        .collect(toList()));

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, readValues);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(
                    rowType(type, type, type),
                    readValues.stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));
        }

        // values wrapped in map
        if (mapTestsEnabled && type.isComparable()) {
            testMapRoundTrip(type, readValues);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, readValues);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    arrayType(type),
                    readValues.stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));
        }
    }

    private void testStructRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        // values in simple struct
        testRoundTripType(
                rowType,
                readValues.stream()
                        .map(OrcTester::toHiveStruct)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(
                    rowType,
                    insertNullEvery(5, readValues).stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));

            // all null values in simple struct
            testRoundTripType(
                    rowType,
                    readValues.stream()
                            .map(value -> toHiveStruct(null))
                            .collect(toList()));
        }
    }

    private void testMapRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type mapType = mapType(type, type);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(
                mapType,
                readValues.stream()
                        .map(value -> toHiveMap(value, readNullKeyValue))
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(
                    mapType,
                    insertNullEvery(5, readValues).stream()
                            .map(value -> toHiveMap(value, readNullKeyValue))
                            .collect(toList()));

            // all null values in simple map
            testRoundTripType(
                    mapType,
                    readValues.stream()
                            .map(value -> toHiveMap(null, readNullKeyValue))
                            .collect(toList()));
        }
    }

    private void testListRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type arrayType = arrayType(type);
        // values in simple list
        testRoundTripType(
                arrayType,
                readValues.stream()
                        .map(OrcTester::toHiveList)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(
                    arrayType,
                    insertNullEvery(5, readValues).stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));

            // all null values in simple list
            testRoundTripType(
                    arrayType,
                    readValues.stream()
                            .map(value -> toHiveList(null))
                            .collect(toList()));
        }
    }

    private void testRoundTripType(Type type, List<?> readValues)
            throws Exception
    {
        // forward order
        assertRoundTrip(type, readValues);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(type, reverse(readValues));
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(type, insertNullEvery(5, readValues));

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(type, insertNullEvery(5, reverse(readValues)));
            }
        }
    }

    public void assertRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        for (Format format : formats) {
            if (!format.supportsType(type)) {
                return;
            }

            MetadataReader metadataReader = format.createMetadataReader();
            for (Compression compression : compressions) {
                try (TempFile tempFile = new TempFile()) {
                    writeOrcColumn(tempFile.getFile(), format, compression, type, readValues.iterator());

                    assertFileContents(type, tempFile, readValues, false, false, metadataReader);

                    if (skipBatchTestsEnabled) {
                        assertFileContents(type, tempFile, readValues, true, false, metadataReader);
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContents(type, tempFile, readValues, false, true, metadataReader);
                    }
                }
            }
        }
    }

    private static void assertFileContents(
            Type type,
            TempFile tempFile,
            List<?> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe,
            MetadataReader metadataReader)
            throws IOException
    {
        try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, metadataReader, createOrcPredicate(type, expectedValues), type)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            Iterator<?> iterator = expectedValues.iterator();
            for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
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
        }
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
                for (Iterator<Entry<?, ?>> iterator = expectedEntries.iterator(); iterator.hasNext(); ) {
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

    public static DataSize writeOrcColumn(File outputFile, Format format, Compression compression, Type type, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter;
        if (DWRF == format) {
            recordWriter = createDwrfRecordWriter(outputFile, compression, type);
        }
        else {
            recordWriter = createOrcRecordWriter(outputFile, format, compression, type);
        }
        return writeOrcFileColumnOld(outputFile, format, recordWriter, type, values);
    }

    public static DataSize writeOrcFileColumnOld(File outputFile, Format format, RecordWriter recordWriter, Type type, Iterator<?> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", type);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        @SuppressWarnings("deprecation") Serializer serializer = format.createSerializer();

        int i = 0;
        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueOld(type, value);
            objectInspector.setStructFieldData(row, fields.get(0), value);

            if (DWRF == format) {
                if (i == 142_345) {
                    setDwrfLowMemoryFlag(recordWriter);
                }
            }
            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
            i++;
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        if (type instanceof VarcharType) {
            return javaStringObjectInspector;
        }
        if (type instanceof CharType) {
            int charLength = ((CharType) type).getLength();
            return new JavaHiveCharObjectInspector(getCharTypeInfo(charLength));
        }
        if (type instanceof VarbinaryType) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DateType.DATE)) {
            return javaDateObjectInspector;
        }
        if (type.equals(TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (type.getTypeSignature().getBase().equals(ARRAY)) {
            return getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        if (type.getTypeSignature().getBase().equals(MAP)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (type.getTypeSignature().getBase().equals(ROW)) {
            return getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(OrcTester::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Object preprocessWriteValueOld(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (type.equals(BOOLEAN)) {
            return value;
        }
        else if (type.equals(TINYINT)) {
            return ((Number) value).byteValue();
        }
        else if (type.equals(SMALLINT)) {
            return ((Number) value).shortValue();
        }
        else if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        else if (type.equals(BIGINT)) {
            return ((Number) value).longValue();
        }
        else if (type.equals(REAL)) {
            return ((Number) value).floatValue();
        }
        else if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        else if (type instanceof VarcharType) {
            return value;
        }
        else if (type instanceof CharType) {
            return new HiveChar((String) value, ((CharType) type).getLength());
        }
        else if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        else if (type.equals(DateType.DATE)) {
            int days = ((SqlDate) value).getDays();
            LocalDate localDate = LocalDate.ofEpochDay(days);
            ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());

            long millis = SECONDS.toMillis(zonedDateTime.toEpochSecond());
            Date date = new Date(0);
            // millis must be set separately to avoid masking
            date.setTime(millis);
            return date;
        }
        else if (type.equals(TIMESTAMP)) {
            long millisUtc = (int) ((SqlTimestamp) value).getMillisUtc();
            return new Timestamp(millisUtc);
        }
        else if (type instanceof DecimalType) {
            return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
        }
        else if (type.getTypeSignature().getBase().equals(ARRAY)) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> preprocessWriteValueOld(elementType, element))
                    .collect(toList());
        }
        else if (type.getTypeSignature().getBase().equals(MAP)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put(preprocessWriteValueOld(keyType, entry.getKey()), preprocessWriteValueOld(valueType, entry.getValue()));
            }
            return newMap;
        }
        else if (type.getTypeSignature().getBase().equals(ROW)) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(preprocessWriteValueOld(fieldTypes.get(fieldId), fieldValues.get(fieldId)));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
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

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, Type type)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties("test", getJavaObjectInspector(type).getTypeName()),
                () -> { }
        );
    }

    private static RecordWriter createDwrfRecordWriter(File outputFile, Compression compressionCodec, Type type)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.default.compress", compressionCodec.name());
        jobConf.set("hive.exec.orc.compress", compressionCodec.name());
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD, 1);
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_ENCODING_INTERVAL, 2);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_BUILD_STRIDE_DICTIONARY, true);

        return new com.facebook.hive.orc.OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != NONE,
                createTableProperties("test", getJavaObjectInspector(type).getTypeName()),
                () -> { }
        );
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, Type type)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(getJavaObjectInspector(type)));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        return orderTableProperties;
    }

    private static <T> List<T> reverse(List<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
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
        });
    }

    private static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static boolean hasType(Type testType, Set<String> baseTypes)
    {
        String testBaseType = testType.getTypeSignature().getBase();
        if (ARRAY.equals(testBaseType)) {
            Type elementType = testType.getTypeParameters().get(0);
            return hasType(elementType, baseTypes);
        }
        if (MAP.equals(testBaseType)) {
            Type keyType = testType.getTypeParameters().get(0);
            Type valueType = testType.getTypeParameters().get(1);
            return hasType(keyType, baseTypes) || hasType(valueType, baseTypes);
        }
        if (ROW.equals(testBaseType)) {
            return testType.getTypeParameters().stream()
                    .anyMatch(fieldType -> hasType(fieldType, baseTypes));
        }

        return baseTypes.contains(testBaseType);
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
}
