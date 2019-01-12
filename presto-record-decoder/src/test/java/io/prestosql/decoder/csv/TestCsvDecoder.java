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
package io.prestosql.decoder.csv;

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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static io.prestosql.decoder.util.DecoderTestUtil.checkIsNull;
import static io.prestosql.decoder.util.DecoderTestUtil.checkValue;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestCsvDecoder
{
    private static final CsvRowDecoderFactory DECODER_FACTORY = new CsvRowDecoderFactory();

    @Test
    public void testSimple()
    {
        String csv = "\"row 1\",row2,\"row3\",100,\"200\",300,4.5";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(2), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", createVarcharType(10), "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", createVarcharType(10), "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BigintType.BIGINT, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle(4, "row5", BigintType.BIGINT, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle(5, "row6", BigintType.BIGINT, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle(6, "row7", DoubleType.DOUBLE, "6", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6, row7);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, "ro");
        checkValue(decodedRow, row2, "row2");
        checkValue(decodedRow, row3, "row3");
        checkValue(decodedRow, row4, 100);
        checkValue(decodedRow, row5, 200);
        checkValue(decodedRow, row6, 300);
        checkValue(decodedRow, row7, 4.5d);
    }

    @Test
    public void testBoolean()
    {
        String csv = "True,False,0,1,\"0\",\"1\",\"true\",\"false\"";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", BooleanType.BOOLEAN, "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BooleanType.BOOLEAN, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", BooleanType.BOOLEAN, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle(4, "row5", BooleanType.BOOLEAN, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle(5, "row6", BooleanType.BOOLEAN, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle(6, "row7", BooleanType.BOOLEAN, "6", null, null, false, false, false);
        DecoderTestColumnHandle row8 = new DecoderTestColumnHandle(7, "row8", BooleanType.BOOLEAN, "7", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6, row7, row8);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, true);
        checkValue(decodedRow, row2, false);
        checkValue(decodedRow, row3, false);
        checkValue(decodedRow, row4, false);
        checkValue(decodedRow, row5, false);
        checkValue(decodedRow, row6, false);
        checkValue(decodedRow, row7, true);
        checkValue(decodedRow, row8, false);
    }

    @Test
    public void testNulls()
    {
        String csv = ",,,";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(10), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BigintType.BIGINT, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", DoubleType.DOUBLE, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkIsNull(decodedRow, row1);
        checkIsNull(decodedRow, row2);
        checkIsNull(decodedRow, row3);
        checkIsNull(decodedRow, row4);
    }

    @Test
    public void testLessTokensThanColumns()
    {
        String csv = "ala,10";

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(10), "0", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", BigintType.BIGINT, "1", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", createVarcharType(10), "2", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(0, "column4", BigintType.BIGINT, "3", null, null, false, false, false);
        DecoderTestColumnHandle column5 = new DecoderTestColumnHandle(0, "column5", DoubleType.DOUBLE, "4", null, null, false, false, false);
        DecoderTestColumnHandle column6 = new DecoderTestColumnHandle(0, "column6", BooleanType.BOOLEAN, "5", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4, column5, column6);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, column1, "ala");
        checkValue(decodedRow, column2, 10);
        checkIsNull(decodedRow, column3);
        checkIsNull(decodedRow, column4);
        checkIsNull(decodedRow, column5);
        checkIsNull(decodedRow, column6);
    }

    @Test
    public void testWrongMappingDefined()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, null, null, null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("mapping not defined for column 'column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "x", null, null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid mapping 'x' for column 'column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "-1", null, null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid mapping '-1' for column 'column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "1:1", null, null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid mapping '1:1' for column 'column'");
    }

    @Test
    public void testInvalidExtraneousParameters()
    {
        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", "format", null, false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected data format 'format' defined for column 'column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", null, "hint", false, false, false))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected format hint 'hint' defined for column 'column'");

        assertThatThrownBy(() -> singleColumnDecoder(BigintType.BIGINT, "0", null, null, false, false, true))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unexpected internal column 'column'");
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BigintType.BIGINT);
        singleColumnDecoder(IntegerType.INTEGER);
        singleColumnDecoder(SmallintType.SMALLINT);
        singleColumnDecoder(TinyintType.TINYINT);
        singleColumnDecoder(BooleanType.BOOLEAN);
        singleColumnDecoder(DoubleType.DOUBLE);
        singleColumnDecoder(createUnboundedVarcharType());
        singleColumnDecoder(createVarcharType(100));

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(RealType.REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DecimalType.createDecimalType(10, 4)));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(VarbinaryType.VARBINARY));
    }

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void singleColumnDecoder(Type columnType)
    {
        singleColumnDecoder(columnType, "0", null, null, false, false, false);
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat, String formatHint, boolean keyDecoder, boolean hidden, boolean internal)
    {
        DECODER_FACTORY.create(emptyMap(), ImmutableSet.of(new DecoderTestColumnHandle(0, "column", columnType, mapping, dataFormat, formatHint, keyDecoder, hidden, internal)));
    }

    @Test
    public void testRuntimeDecodingFailure()
    {
        assertRuntimeDecodingFailure(() -> fieldValueDecoderFor(BigintType.BIGINT, "blah").getLong());
    }

    private FieldValueProvider fieldValueDecoderFor(BigintType type, String csv)
    {
        DecoderTestColumnHandle column = new DecoderTestColumnHandle(0, "column", type, "0", null, null, false, false, false);
        Set<DecoderColumnHandle> columns = ImmutableSet.of(column);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null)
                .orElseThrow(AssertionError::new);
        return decodedRow.get(column);
    }

    private void assertRuntimeDecodingFailure(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("could not parse value .* as .* for column .*");
    }
}
