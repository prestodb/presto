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
package com.facebook.presto.decoder.avro;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
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

    private static final Type VACHAR_MAP_TYPE = FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("map<varchar,varchar>"));
    private static final Type DOUBLE_MAP_TYPE = FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("map<varchar,double>"));

    private static Schema getAvroSchema(Map<String, String> fields)
    {
        Schema.Parser parser = new Schema.Parser();
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = getFieldBuilder();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            SchemaBuilder.FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(field.getKey());
            Schema fieldSchema = parser.parse(field.getValue());
            SchemaBuilder.GenericDefault<Schema> genericDefault = fieldBuilder.type(fieldSchema);
            switch (fieldSchema.getType()) {
                case ARRAY:
                    genericDefault.withDefault(ImmutableList.of());
                    break;
                case MAP:
                    genericDefault.withDefault(ImmutableMap.of());
                    break;
                case UNION:
                    if (fieldSchema.getTypes().stream()
                            .map(Schema::getType)
                            .anyMatch(Schema.Type.NULL::equals)) {
                        genericDefault.withDefault(null);
                    }
                    else {
                        genericDefault.noDefault();
                    }
                    break;
                case NULL:
                    genericDefault.withDefault(null);
                    break;
                default:
                    genericDefault.noDefault();
            }
        }
        return fieldAssembler.endRecord();
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumns(Set<DecoderColumnHandle> columns, Map<String, String> fieldSchema, Map<String, Object> fieldValue)
    {
        Schema schema = getAvroSchema(fieldSchema);
        byte[] avroData = buildAvroData(schema, fieldValue);

        return decodeRow(
                avroData,
                columns,
                ImmutableMap.of(DATA_SCHEMA, schema.toString()));
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
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
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
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .endRecord(),
                "string_field", "string_field_value");
        String addedColumnSchema = getFieldBuilder()
                .name("string_field").type().stringType().noDefault()
                .name("string_field_added").type().optional().stringType()
                .endRecord().toString();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                originalData,
                ImmutableSet.of(originalColumn, newlyAddedColumn),
                ImmutableMap.of(DATA_SCHEMA, addedColumnSchema));

        assertEquals(decodedRow.size(), 2);
        checkValue(decodedRow, originalColumn, "string_field_value");
        checkIsNull(decodedRow, newlyAddedColumn);
    }

    @Test
    public void testEnumDecodedAsVarchar()
    {
        Schema schema = SchemaBuilder.record("record")
                .fields()
                .name("enum_field")
                .type()
                .enumeration("Weekday")
                .symbols("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
                .noDefault()
                .endRecord();
        Schema enumType = schema.getField("enum_field").schema();
        GenericData.EnumSymbol enumValue = new GenericData.EnumSymbol(enumType, "Wednesday");
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARCHAR, "enum_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "enum_field", enumType.toString(), enumValue);

        checkValue(decodedRow, row, "Wednesday");
    }

    @Test
    public void testSchemaEvolutionRenamingColumn()
            throws Exception
    {
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .endRecord(),
                "string_field", "string_field_value");

        DecoderTestColumnHandle renamedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field_renamed", null, null, false, false, false);
        String renamedColumnSchema = getFieldBuilder()
                .name("string_field_renamed").type().optional().stringType()
                .endRecord()
                .toString();
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
        byte[] originalData = buildAvroData(getFieldBuilder()
                        .name("string_field").type().stringType().noDefault()
                        .name("string_field_to_be_removed").type().optional().stringType()
                        .endRecord(),
                ImmutableMap.of(
                        "string_field", "string_field_value",
                        "string_field_to_be_removed", "removed_field_value"));

        DecoderTestColumnHandle evolvedColumn = new DecoderTestColumnHandle(0, "row0", VARCHAR, "string_field", null, null, false, false, false);
        String removedColumnSchema = getFieldBuilder()
                .name("string_field").type().stringType().noDefault()
                .endRecord()
                .toString();
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
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_long_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_long_field", 100);

        DecoderTestColumnHandle longColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", BIGINT, "int_to_long_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_long_field").type().longType().noDefault()
                .endRecord()
                .toString();
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
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_double_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_double_field", 100);

        DecoderTestColumnHandle doubleColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", DOUBLE, "int_to_double_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_double_field").type().doubleType().noDefault()
                .endRecord()
                .toString();
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
        byte[] originalIntData = buildAvroData(getFieldBuilder()
                        .name("int_to_string_field").type().intType().noDefault()
                        .endRecord(),
                "int_to_string_field", 100);

        DecoderTestColumnHandle stringColumnReadingIntData = new DecoderTestColumnHandle(0, "row0", VARCHAR, "int_to_string_field", null, null, false, false, false);
        String changedTypeSchema = getFieldBuilder()
                .name("int_to_string_field").type().stringType().noDefault()
                .endRecord()
                .toString();

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
    public void testIntDecodedAsInteger()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", INTEGER, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 100_000);

        checkValue(decodedRow, row, 100_000);
    }

    @Test
    public void testIntDecodedAsSmallInt()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", SMALLINT, "id", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "id", "\"int\"", 1000);

        checkValue(decodedRow, row, 1000);
    }

    @Test
    public void testIntDecodedAsTinyInt()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", TINYINT, "id", null, null, false, false, false);
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
    public void testFloatDecodedAsReal()
    {
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", REAL, "float_field", null, null, false, false, false);
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
    public void testFixedDecodedAsVarbinary()
    {
        Schema schema = SchemaBuilder.record("record")
                .fields().name("fixed_field")
                .type()
                .fixed("fixed5")
                .size(5)
                .noDefault()
                .endRecord();
        Schema fixedType = schema.getField("fixed_field").schema();
        GenericData.Fixed fixedValue = new GenericData.Fixed(schema.getField("fixed_field").schema());
        byte[] bytes = {5, 4, 3, 2, 1};
        fixedValue.bytes(bytes);
        DecoderTestColumnHandle row = new DecoderTestColumnHandle(0, "row", VARBINARY, "fixed_field", null, null, false, false, false);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = buildAndDecodeColumn(row, "fixed_field", fixedType.toString(), fixedValue);

        checkValue(decodedRow, row, Slices.wrappedBuffer(bytes));
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
                "  \"namespace\" : \"com.facebook.presto.decoder.avro\"," +
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
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4)));
    }

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private static SchemaBuilder.FieldAssembler<Schema> getFieldBuilder()
    {
        return SchemaBuilder.record("test_schema")
                .namespace("com.facebook.presto.decoder.avro")
                .fields();
    }

    private void singleColumnDecoder(Type columnType)
    {
        String someSchema = getFieldBuilder()
                .name("dummy").type().longType().noDefault()
                .endRecord()
                .toString();
        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, "0", null, null, false, false, false)));
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat, String formatHint, boolean keyDecoder, boolean hidden, boolean internal)
    {
        String someSchema = getFieldBuilder()
                .name("dummy").type().longType().noDefault()
                .endRecord()
                .toString();
        DECODER_FACTORY.create(ImmutableMap.of(DATA_SCHEMA, someSchema), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, keyDecoder, hidden, internal)));
    }
}
