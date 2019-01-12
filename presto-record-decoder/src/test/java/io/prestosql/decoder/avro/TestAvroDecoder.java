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
package io.prestosql.decoder.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.DecoderTestColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.decoder.util.DecoderTestUtil.checkIsNull;
import static io.prestosql.decoder.util.DecoderTestUtil.checkValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingEnvironment.TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAvroDecoder
{
    private static final String DATA_SCHEMA = "dataSchema";
    private static final AvroRowDecoderFactory DECODER_FACTORY = new AvroRowDecoderFactory();

    private static final Type VACHAR_MAP_TYPE = TYPE_MANAGER.getType(parseTypeSignature("map<varchar,varchar>"));
    private static final Type DOUBLE_MAP_TYPE = TYPE_MANAGER.getType(parseTypeSignature("map<varchar,double>"));
    private static final Type REAL_MAP_TYPE = TYPE_MANAGER.getType(parseTypeSignature("map<varchar,real>"));

    private static String getAvroSchema(String name, String dataType)
    {
        return getAvroSchema(ImmutableMap.of(name, dataType));
    }

    private static String getAvroSchema(Map<String, String> fields)
    {
        String fieldSchema = fields.entrySet().stream()
                .map(entry -> "{\"name\": \"" + entry.getKey() + "\",\"type\": " + entry.getValue() + ",\"default\": null}")
                .collect(Collectors.joining(","));

        return "{\"type\" : \"record\"," +
                "  \"name\" : \"test_schema\"," +
                "  \"namespace\" : \"io.prestosql.decoder.avro\"," +
                "  \"fields\" :" +
                "  [" +
                fieldSchema +
                "  ]}";
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumns(Set<DecoderColumnHandle> columns, Map<String, String> fieldSchema, Map<String, Object> fieldValue)
    {
        String schema = getAvroSchema(fieldSchema);
        byte[] avroData = buildAvroData(new Schema.Parser().parse(schema), fieldValue);

        return decodeRow(
                avroData,
                columns,
                ImmutableMap.of(DATA_SCHEMA, schema));
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumn(DecoderTestColumnHandle column, String columnName, String columnType, Object actualValue)
    {
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumns(
                ImmutableSet.of(column),
                ImmutableMap.of(columnName, columnType),
                ImmutableMap.of(columnName, actualValue));

        assertEquals(decodedRow.size(), 1);
        return decodedRow;
    }

    private static Map<DecoderColumnHandle, FieldValueProvider> decodeRow(byte[] avroData, Set<DecoderColumnHandle> columns, Map<String, String> dataParams)
    {
        RowDecoder rowDecoder = DECODER_FACTORY.create(dataParams, columns);
        return rowDecoder.decodeRow(avroData, null)
                .orElseThrow(AssertionError::new);
    }

    private static byte[] buildAvroData(Schema schema, String name, Object value)
    {
        return buildAvroData(schema, ImmutableMap.of(name, value));
    }

    private static byte[] buildAvroData(Schema schema, Map<String, Object> values)
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        buildAvroRecord(schema, outputStream, values);
        return outputStream.toByteArray();
    }

    private static GenericData.Record buildAvroRecord(Schema schema, ByteArrayOutputStream outputStream, Map<String, Object> values)
    {
        GenericData.Record record = new GenericData.Record(schema);
        values.forEach(record::put);
        try {
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));

            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to convert to Avro.", e);
        }
        return record;
    }

    @Test
    public void testStringDecodedAsVarchar()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR, "string_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "string_field", "\"string\"", "Mon Jul 28 20:38:07 +0000 2014");

        checkValue(decodedRow, row, "Mon Jul 28 20:38:07 +0000 2014");
    }

    @Test
    public void testSchemaEvolutionAddingColumn()
            throws Exception
    {
        DecoderTestColumnHandle originalColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field", null, null, false, false, false);
        DecoderTestColumnHandle newlyAddedColumn = new DecoderTestColumnHandle(1, "row1", VARCHAR, "string_field_added", null, null, false, false, false);

        // the decoded avro data file does not have string_field_added
        byte[] originalData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema("string_field", "\"string\"")),
                "string_field", "string_field_value");
        String addedColumnSchema = getAvroSchema(ImmutableMap.of(
                "string_field", "\"string\"",
                "string_field_added", "[\"null\", \"string\"]"));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                originalData,
                ImmutableSet.of(originalColumn, newlyAddedColumn),
                ImmutableMap.of(DATA_SCHEMA, addedColumnSchema));

        assertEquals(decodedRow.size(), 2);
        checkValue(decodedRow, originalColumn, "string_field_value");
        checkIsNull(decodedRow, newlyAddedColumn);
    }

    @Test
    public void testSchemaEvolutionRenamingColumn()
            throws Exception
    {
        byte[] originalData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema("string_field", "\"string\"")),
                "string_field", "string_field_value");

        DecoderTestColumnHandle renamedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field_renamed", null, null, false, false, false);
        String renamedColumnSchema = getAvroSchema("string_field_renamed", "[\"null\", \"string\"]");
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalData,
                ImmutableSet.of(renamedColumn),
                ImmutableMap.of(DATA_SCHEMA, renamedColumnSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkIsNull(decodedEvolvedRow, renamedColumn);
    }

    @Test
    public void testSchemaEvolutionRemovingColumn()
            throws Exception
    {
        byte[] originalData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema(ImmutableMap.of(
                        "string_field", "\"string\"",
                        "string_field_to_be_removed", "[\"null\", \"string\"]"))),
                ImmutableMap.of(
                        "string_field", "string_field_value",
                        "string_field_to_be_removed", "removed_field_value"));

        DecoderTestColumnHandle evolvedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field", null, null, false, false, false);
        String removedColumnSchema = getAvroSchema("string_field", "\"string\"");
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalData,
                ImmutableSet.of(evolvedColumn),
                ImmutableMap.of(DATA_SCHEMA, removedColumnSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, evolvedColumn, "string_field_value");
    }

    @Test
    public void testSchemaEvolutionIntToLong()
            throws Exception
    {
        byte[] originalIntData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema("int_to_long_field", "\"int\"")),
                "int_to_long_field", 100);

        DecoderTestColumnHandle longColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", BIGINT, "int_to_long_field", null, null, false, false, false);
        String changedTypeSchema = getAvroSchema("int_to_long_field", "\"long\"");
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalIntData,
                ImmutableSet.of(longColumnReadingIntData),
                ImmutableMap.of(DATA_SCHEMA, changedTypeSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, longColumnReadingIntData, 100);
    }

    @Test
    public void testSchemaEvolutionIntToDouble()
            throws Exception
    {
        byte[] originalIntData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema("int_to_double_field", "\"int\"")),
                "int_to_double_field", 100);

        DecoderTestColumnHandle doubleColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", DOUBLE, "int_to_double_field", null, null, false, false, false);
        String changedTypeSchema = getAvroSchema("int_to_double_field", "\"double\"");
        Map<DecoderColumnHandle, FieldValueProvider> decodedEvolvedRow = decodeRow(
                originalIntData,
                ImmutableSet.of(doubleColumnReadingIntData),
                ImmutableMap.of(DATA_SCHEMA, changedTypeSchema));

        assertEquals(decodedEvolvedRow.size(), 1);
        checkValue(decodedEvolvedRow, doubleColumnReadingIntData, 100.0);
    }

    @Test
    public void testSchemaEvolutionToIncompatibleType()
            throws Exception
    {
        byte[] originalIntData = buildAvroData(new Schema.Parser().parse(
                getAvroSchema("int_to_string_field", "\"int\"")),
                "int_to_string_field", 100);

        DecoderTestColumnHandle stringColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", VARCHAR, "int_to_string_field", null, null, false, false, false);
        String changedTypeSchema = getAvroSchema("int_to_string_field", "\"string\"");

        assertThatThrownBy(() -> decodeRow(originalIntData, ImmutableSet.of(stringColumnReadingIntData), ImmutableMap.of(DATA_SCHEMA, changedTypeSchema)))
                .isInstanceOf(PrestoException.class)
                .hasCauseExactlyInstanceOf(AvroTypeException.class)
                .hasStackTraceContaining("Found int, expecting string")
                .hasMessageMatching("Decoding Avro record failed.");
    }

    @Test
    public void testLongDecodedAsBigint()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"long\"", 493857959588286460L);

        checkValue(decodedRow, row, 493857959588286460L);
    }

    @Test
    public void testIntDecodedAsBigint()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 100);

        checkValue(decodedRow, row, 100);
    }

    @Test
    public void testFloatDecodedAsDouble()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", DOUBLE, "float_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "float_field", "\"float\"", 10.2f);

        checkValue(decodedRow, row, 10.2);
    }

    @Test
    public void testBytesDecodedAsVarbinary()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARBINARY, "encoded", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "encoded", "\"bytes\"", ByteBuffer.wrap("mytext".getBytes(UTF_8)));

        checkValue(decodedRow, row, "mytext");
    }

    @Test
    public void testDoubleDecodedAsDouble()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", DOUBLE, "double_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "double_field", "\"double\"", 56.898);

        checkValue(decodedRow, row, 56.898);
    }

    @Test
    public void testStringDecodedAsVarcharN()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", createVarcharType(10), "varcharn_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "varcharn_field", "\"string\"", "abcdefghijklmno");

        checkValue(decodedRow, row, "abcdefghij");
    }

    @Test
    public void testNestedRecord()
            throws Exception
    {
        String schema = "{\"type\" : \"record\", " +
                "  \"name\" : \"nested_schema\"," +
                "  \"namespace\" : \"io.prestosql.decoder.avro\"," +
                "  \"fields\" :" +
                "  [{" +
                "            \"name\":\"nested\"," +
                "            \"type\":{" +
                "                      \"type\":\"record\"," +
                "                      \"name\":\"Nested\"," +
                "                      \"fields\":" +
                "                      [" +
                "                          {" +
                "                              \"name\":\"id\"," +
                "                              \"type\":[\"long\", \"null\"]" +
                "                          }" +
                "                      ]" +
                "                  }" +
                "  }]}";
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", BIGINT, "nested/id", null, null, false, false, false);

        Schema nestedSchema = new Schema.Parser().parse(schema);
        Schema userSchema = nestedSchema.getField("nested").schema();
        GenericData.Record userRecord = buildAvroRecord(userSchema, new ByteArrayOutputStream(), ImmutableMap.of("id", 98247748L));
        byte[] avroData = buildAvroData(nestedSchema, "nested", userRecord);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                avroData,
                ImmutableSet.of(row),
                ImmutableMap.of(DATA_SCHEMA, schema));

        assertEquals(decodedRow.size(), 1);

        checkValue(decodedRow, row, 98247748);
    }

    @Test
    public void testNonExistentFieldsAreNull()
            throws Exception
    {
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(100), "very/deep/varchar", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BIGINT, "no_bigint", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", DOUBLE, "double_record/is_missing", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BOOLEAN, "hello", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow1 = buildAndDecodeColumn(row1, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow2 = buildAndDecodeColumn(row2, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow3 = buildAndDecodeColumn(row3, "dummy", "\"long\"", 0L);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow4 = buildAndDecodeColumn(row4, "dummy", "\"long\"", 0L);

        checkIsNull(decodedRow1, row1);
        checkIsNull(decodedRow2, row2);
        checkIsNull(decodedRow3, row3);
        checkIsNull(decodedRow4, row4);
    }

    @Test
    public void testRuntimeDecodingFailure()
    {
        DecoderTestColumnHandle booleanColumn = new DecoderTestColumnHandle(0, "some_column", BOOLEAN, "long_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(booleanColumn, "long_field", "\"long\"", (long) 1);

        assertThatThrownBy(decodedRow.get(booleanColumn)::getBoolean)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("cannot decode object of 'class java.lang.Long' as 'boolean' for column 'some_column'");
    }

    @Test
    public void testArrayDecodedAsArray()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(BIGINT), "array_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", "{\"type\": \"array\", \"items\": \"long\"}", ImmutableList.of(114L, 136L));
        checkArrayValue(decodedRow, row, new long[] {114, 136});
    }

    @Test
    public void testArrayWithNulls()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", new ArrayType(BIGINT), "array_field", null, null, false, false, false);

        List<Long> values = new ArrayList<>();
        values.add(null);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "array_field", "{\"type\": \"array\", \"items\": \"null\"}", values);
        checkArrayItemIsNull(decodedRow, row, new long[] {0});
    }

    @Test
    public void testMapDecodedAsMap()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VACHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"string\"}", ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
        checkMapValue(decodedRow, row, ImmutableMap.of(
                "key1", "abc",
                "key2", "def",
                "key3", "zyx"));
    }

    @Test
    public void testMapWithNull()
            throws Exception
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VACHAR_MAP_TYPE, "map_field", null, null, false, false, false);

        Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("key1", null);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "map_field", "{\"type\": \"map\", \"values\": \"null\"}", expectedValues);

        checkMapValue(decodedRow, row, expectedValues);
    }

    private static void checkArrayValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long[] expected)
    {
        Block actualBlock = getBlock(decodedRow, handle);
        assertEquals(actualBlock.getPositionCount(), expected.length);

        for (int i = 0; i < actualBlock.getPositionCount(); i++) {
            assertFalse(actualBlock.isNull(i));
            assertEquals(BIGINT.getLong(actualBlock, i), expected[i]);
        }
    }

    private static void checkArrayItemIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long[] expected)
    {
        Block actualBlock = getBlock(decodedRow, handle);
        assertEquals(actualBlock.getPositionCount(), expected.length);

        for (int i = 0; i < actualBlock.getPositionCount(); i++) {
            assertTrue(actualBlock.isNull(i));
            assertEquals(BIGINT.getLong(actualBlock, i), expected[i]);
        }
    }

    private static void checkMapValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderTestColumnHandle handle, Map<String, String> expected)
    {
        Block actualBlock = getBlock(decodedRow, handle);
        assertEquals(actualBlock.getPositionCount(), expected.size() * 2);

        for (int i = 0; i < actualBlock.getPositionCount(); i += 2) {
            String actualKey = VARCHAR.getSlice(actualBlock, i).toStringUtf8();
            String actualValue;
            if (actualBlock.isNull(i + 1)) {
                actualValue = null;
            }
            else {
                actualValue = VARCHAR.getSlice(actualBlock, i + 1).toStringUtf8();
            }
            assertTrue(expected.containsKey(actualKey));
            assertEquals(actualValue, expected.get(actualKey));
        }
    }

    private static Block getBlock(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        return provider.getBlock();
    }

    @Test
    public void testInvalidExtraneousParameters()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "mapping", null, "hint", false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected format hint 'hint' defined for column 'some_column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "mapping", null, null, false, false, true))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected internal column 'some_column'");
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BigintType.BIGINT);
        singleColumnDecoder(VarbinaryType.VARBINARY);
        singleColumnDecoder(BooleanType.BOOLEAN);
        singleColumnDecoder(DoubleType.DOUBLE);
        singleColumnDecoder(createUnboundedVarcharType());
        singleColumnDecoder(createVarcharType(100));
        singleColumnDecoder(new ArrayType(BigintType.BIGINT));
        singleColumnDecoder(VACHAR_MAP_TYPE);
        singleColumnDecoder(DOUBLE_MAP_TYPE);

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(RealType.REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(IntegerType.INTEGER));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(SmallintType.SMALLINT));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TinyintType.TINYINT));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4)));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(new ArrayType(RealType.REAL)));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(REAL_MAP_TYPE));
    }

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void singleColumnDecoder(Type columnType)
    {
        String someSchema = getAvroSchema("dummy", "\"long\"");
        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, "0", null, null, false, false, false)));
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat, String formatHint, boolean keyDecoder, boolean hidden, boolean internal)
    {
        String someSchema = getAvroSchema("dummy", "\"long\"");
        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, keyDecoder, hidden, internal)));
    }
}
