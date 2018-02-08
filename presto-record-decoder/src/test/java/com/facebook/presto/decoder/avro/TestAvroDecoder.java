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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAvroDecoder
{
    private static final String DATA_SCHEMA = "dataSchema";
    private static final AvroRowDecoderFactory DECODER_FACTORY = new AvroRowDecoderFactory();

    private static String getAvroSchema(String name, String dataType)
    {
        return "{\"type\" : \"record\"," +
                "  \"name\" : \"test_schema\"," +
                "  \"namespace\" : \"com.facebook.presto.decoder.avro\"," +
                "  \"fields\" :" +
                "  [" +
                "   {\"name\": \"" + name + "\",\"type\": " + dataType + "} " +
                "  ]}";
    }

    private Map<DecoderColumnHandle, FieldValueProvider> buildAndDecodeColumn(DecoderTestColumnHandle column, String columnName, String columnType, Object actualValue)
    {
        String schema = getAvroSchema(columnName, columnType);
        byte[] avroData = buildAvroData(new Schema.Parser().parse(schema), columnName, actualValue);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decodeRow(
                avroData,
                ImmutableSet.of(column),
                ImmutableMap.of(DATA_SCHEMA, schema));

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
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        buildAvroRecord(schema, outputStream, ImmutableMap.of(name, value));
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
            throw new RuntimeException("Failed to convert to AVRO.", e);
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

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(RealType.REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(IntegerType.INTEGER));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(SmallintType.SMALLINT));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TinyintType.TINYINT));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4)));
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
