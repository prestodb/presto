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
package io.prestosql.decoder.raw;

import com.google.common.collect.ImmutableSet;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.DecoderTestColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
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
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static io.prestosql.decoder.util.DecoderTestUtil.checkIsNull;
import static io.prestosql.decoder.util.DecoderTestUtil.checkValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestRawDecoder
{
    private static final RawRowDecoderFactory DECODER_FACTORY = new RawRowDecoderFactory();

    @Test
    public void testEmptyRecord()
    {
        byte[] emptyRow = new byte[0];
        DecoderTestColumnHandle column = new DecoderTestColumnHandle(0, "row1", createUnboundedVarcharType(), null, "BYTE", null, false, false, false);
        Set<DecoderColumnHandle> columns = ImmutableSet.of(column);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(emptyRow, null)
                .orElseThrow(AssertionError::new);

        checkIsNull(decodedRow, column);
    }

    @Test
    public void testSimple()
    {
        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.putLong(4815162342L); // 0 - 7
        buf.putInt(12345678); // 8 - 11
        buf.putShort((short) 4567); // 12 - 13
        buf.put((byte) 123); // 14
        buf.put("Ich bin zwei Oeltanks".getBytes(StandardCharsets.UTF_8)); // 15+

        byte[] row = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, row, 0, buf.position());

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", BigintType.BIGINT, "0", "LONG", null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BigintType.BIGINT, "8", "INT", null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", BigintType.BIGINT, "12", "SHORT", null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BigintType.BIGINT, "14", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle(4, "row5", createVarcharType(10), "15", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(row, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, 4815162342L);
        checkValue(decodedRow, row2, 12345678);
        checkValue(decodedRow, row3, 4567);
        checkValue(decodedRow, row4, 123);
        checkValue(decodedRow, row5, "Ich bin zw");
    }

    @Test
    public void testFixedWithString()
    {
        String str = "Ich bin zwei Oeltanks";
        byte[] row = str.getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(100), null, null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", createVarcharType(100), "0", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", createVarcharType(100), "0:4", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", createVarcharType(100), "5:8", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(row, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, str);
        checkValue(decodedRow, row2, str);
        // these only work for single byte encodings...
        checkValue(decodedRow, row3, str.substring(0, 4));
        checkValue(decodedRow, row4, str.substring(5, 8));
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Test
    public void testFloatStuff()
    {
        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.putDouble(Math.PI);
        buf.putFloat((float) Math.E);
        buf.putDouble(Math.E);

        byte[] row = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, row, 0, buf.position());

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", DOUBLE, null, "DOUBLE", null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", DOUBLE, "8", "FLOAT", null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(row, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, Math.PI);
        checkValue(decodedRow, row2, Math.E);
    }

    @Test
    public void testBooleanStuff()
    {
        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.put((byte) 127); // offset 0
        buf.putLong(0); // offset 1
        buf.put((byte) 126); // offset 9
        buf.putLong(1); // offset 10

        buf.put((byte) 125); // offset 18
        buf.putInt(0); // offset 19
        buf.put((byte) 124); // offset 23
        buf.putInt(1); // offset 24

        buf.put((byte) 123); // offset 28
        buf.putShort((short) 0); // offset 29
        buf.put((byte) 122); // offset 31
        buf.putShort((short) 1); // offset 32

        buf.put((byte) 121); // offset 34
        buf.put((byte) 0); // offset 35
        buf.put((byte) 120); // offset 36
        buf.put((byte) 1); // offset 37

        byte[] row = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, row, 0, buf.position());

        DecoderTestColumnHandle row01 = new DecoderTestColumnHandle(0, "row01", BigintType.BIGINT, "0", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row02 = new DecoderTestColumnHandle(1, "row02", BooleanType.BOOLEAN, "1", "LONG", null, false, false, false);
        DecoderTestColumnHandle row03 = new DecoderTestColumnHandle(2, "row03", BigintType.BIGINT, "9", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row04 = new DecoderTestColumnHandle(3, "row04", BooleanType.BOOLEAN, "10", "LONG", null, false, false, false);

        DecoderTestColumnHandle row11 = new DecoderTestColumnHandle(4, "row11", BigintType.BIGINT, "18", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row12 = new DecoderTestColumnHandle(5, "row12", BooleanType.BOOLEAN, "19", "INT", null, false, false, false);
        DecoderTestColumnHandle row13 = new DecoderTestColumnHandle(6, "row13", BigintType.BIGINT, "23", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row14 = new DecoderTestColumnHandle(7, "row14", BooleanType.BOOLEAN, "24", "INT", null, false, false, false);

        DecoderTestColumnHandle row21 = new DecoderTestColumnHandle(8, "row21", BigintType.BIGINT, "28", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row22 = new DecoderTestColumnHandle(9, "row22", BooleanType.BOOLEAN, "29", "SHORT", null, false, false, false);
        DecoderTestColumnHandle row23 = new DecoderTestColumnHandle(10, "row23", BigintType.BIGINT, "31", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row24 = new DecoderTestColumnHandle(11, "row24", BooleanType.BOOLEAN, "32", "SHORT", null, false, false, false);

        DecoderTestColumnHandle row31 = new DecoderTestColumnHandle(12, "row31", BigintType.BIGINT, "34", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row32 = new DecoderTestColumnHandle(13, "row32", BooleanType.BOOLEAN, "35", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row33 = new DecoderTestColumnHandle(14, "row33", BigintType.BIGINT, "36", "BYTE", null, false, false, false);
        DecoderTestColumnHandle row34 = new DecoderTestColumnHandle(15, "row34", BooleanType.BOOLEAN, "37", "BYTE", null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row01,
                row02,
                row03,
                row04,
                row11,
                row12,
                row13,
                row14,
                row21,
                row22,
                row23,
                row24,
                row31,
                row32,
                row33,
                row34);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(row, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row01, 127);
        checkValue(decodedRow, row02, false);
        checkValue(decodedRow, row03, 126);
        checkValue(decodedRow, row04, true);

        checkValue(decodedRow, row11, 125);
        checkValue(decodedRow, row12, false);
        checkValue(decodedRow, row13, 124);
        checkValue(decodedRow, row14, true);

        checkValue(decodedRow, row21, 123);
        checkValue(decodedRow, row22, false);
        checkValue(decodedRow, row23, 122);
        checkValue(decodedRow, row24, true);

        checkValue(decodedRow, row31, 121);
        checkValue(decodedRow, row32, false);
        checkValue(decodedRow, row33, 120);
        checkValue(decodedRow, row34, true);
    }

    @Test
    public void testMappingForFixedWidthTypesValidation()
    {
        singleColumnDecoder(BigintType.BIGINT, "0", "BYTE");
        singleColumnDecoder(BigintType.BIGINT, "0:1", "BYTE");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:0", "BYTE"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:2", "BYTE"));

        singleColumnDecoder(BigintType.BIGINT, "0", "SHORT");
        singleColumnDecoder(BigintType.BIGINT, "0:2", "SHORT");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:1", "SHORT"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:3", "SHORT"));

        singleColumnDecoder(BigintType.BIGINT, "0", "INT");
        singleColumnDecoder(BigintType.BIGINT, "0:4", "INT");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:3", "INT"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:5", "INT"));

        singleColumnDecoder(BigintType.BIGINT, "0", "LONG");
        singleColumnDecoder(BigintType.BIGINT, "0:8", "LONG");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:7", "LONG"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:9", "LONG"));

        singleColumnDecoder(BigintType.BIGINT, "0", "LONG");
        singleColumnDecoder(BigintType.BIGINT, "0:8", "LONG");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:7", "LONG"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0:9", "LONG"));

        singleColumnDecoder(DoubleType.DOUBLE, "0", "FLOAT");
        singleColumnDecoder(DoubleType.DOUBLE, "0:4", "FLOAT");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0:3", "FLOAT"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0:5", "FLOAT"));

        singleColumnDecoder(DoubleType.DOUBLE, "0", "DOUBLE");
        singleColumnDecoder(DoubleType.DOUBLE, "0:8", "DOUBLE");
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0:7", "DOUBLE"));
        assertMappingDoesNotMatchDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0:9", "DOUBLE"));
    }

    private void assertMappingDoesNotMatchDataFormatException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Bytes mapping for column 'some_column' does not match dataFormat");
    }

    @Test
    public void testInvalidMapping()
    {
        assertThatThrownBy(() -> singleColumnDecoder(DoubleType.DOUBLE, "x", "DOUBLE"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("invalid mapping format 'x' for column 'some_column'");
    }

    @Test
    public void testInvalidDataFormat()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", "format", null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid dataFormat 'format' for column 'some_column'");
    }

    @Test
    public void testInvalidExtraneousParameters()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", null, "hint", false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected format hint 'hint' defined for column 'some_column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", null, null, false, false, true))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected internal column 'some_column'");
    }

    @Test
    public void testTypeMatchesDataFormatValidation()
    {
        singleColumnDecoder(BigintType.BIGINT, "0", "BYTE");
        singleColumnDecoder(BigintType.BIGINT, "0", "SHORT");
        singleColumnDecoder(BigintType.BIGINT, "0", "INT");
        singleColumnDecoder(BigintType.BIGINT, "0", "LONG");
        singleColumnDecoder(IntegerType.INTEGER, "0", "BYTE");
        singleColumnDecoder(IntegerType.INTEGER, "0", "SHORT");
        singleColumnDecoder(IntegerType.INTEGER, "0", "INT");
        singleColumnDecoder(SmallintType.SMALLINT, "0", "BYTE");
        singleColumnDecoder(SmallintType.SMALLINT, "0", "SHORT");
        singleColumnDecoder(TinyintType.TINYINT, "0", "BYTE");
        singleColumnDecoder(BooleanType.BOOLEAN, "0", "BYTE");
        singleColumnDecoder(BooleanType.BOOLEAN, "0", "SHORT");
        singleColumnDecoder(BooleanType.BOOLEAN, "0", "INT");
        singleColumnDecoder(BooleanType.BOOLEAN, "0", "LONG");
        singleColumnDecoder(DoubleType.DOUBLE, "0", "DOUBLE");
        singleColumnDecoder(DoubleType.DOUBLE, "0", "FLOAT");
        singleColumnDecoder(createUnboundedVarcharType(), "0", "BYTE");
        singleColumnDecoder(createVarcharType(100), "0", "BYTE");

        assertWrongDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0", "FLOAT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(BigintType.BIGINT, "0", "DOUBLE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(IntegerType.INTEGER, "0", "FLOAT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(IntegerType.INTEGER, "0", "DOUBLE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(IntegerType.INTEGER, "0", "LONG"));
        assertWrongDataFormatException(() -> singleColumnDecoder(SmallintType.SMALLINT, "0", "FLOAT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(SmallintType.SMALLINT, "0", "DOUBLE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(SmallintType.SMALLINT, "0", "LONG"));
        assertWrongDataFormatException(() -> singleColumnDecoder(SmallintType.SMALLINT, "0", "INT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(TinyintType.TINYINT, "0", "FLOAT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(TinyintType.TINYINT, "0", "DOUBLE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(TinyintType.TINYINT, "0", "LONG"));
        assertWrongDataFormatException(() -> singleColumnDecoder(TinyintType.TINYINT, "0", "INT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(TinyintType.TINYINT, "0", "SHORT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0", "LONG"));
        assertWrongDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0", "INT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0", "SHORT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(DoubleType.DOUBLE, "0", "BYTE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(createVarcharType(100), "0", "FLOAT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(createVarcharType(100), "0", "DOUBLE"));
        assertWrongDataFormatException(() -> singleColumnDecoder(createVarcharType(100), "0", "LONG"));
        assertWrongDataFormatException(() -> singleColumnDecoder(createVarcharType(100), "0", "INT"));
        assertWrongDataFormatException(() -> singleColumnDecoder(createVarcharType(100), "0", "SHORT"));
    }

    private void assertWrongDataFormatException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("Wrong dataFormat .* specified for column .*");
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BigintType.BIGINT, "0", "LONG");
        singleColumnDecoder(IntegerType.INTEGER, "0", "INT");
        singleColumnDecoder(SmallintType.SMALLINT, "0", "SHORT");
        singleColumnDecoder(TinyintType.TINYINT, "0", "BYTE");
        singleColumnDecoder(BooleanType.BOOLEAN, "0", "LONG");
        singleColumnDecoder(DoubleType.DOUBLE, "0", "DOUBLE");
        singleColumnDecoder(createUnboundedVarcharType(), "0", "BYTE");
        singleColumnDecoder(createVarcharType(100), "0", "BYTE");

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(RealType.REAL, "0", "BYTE"));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4), "0", "BYTE"));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(VarbinaryType.VARBINARY, "0", "BYTE"));
    }

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat)
    {
        singleColumnDecoder(columnType, mapping, dataFormat, null, false, false, false);
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat, String formatHint, boolean keyDecoder, boolean hidden, boolean internal)
    {
        DECODER_FACTORY.create(emptyMap(), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, keyDecoder, hidden, internal)));
    }
}
