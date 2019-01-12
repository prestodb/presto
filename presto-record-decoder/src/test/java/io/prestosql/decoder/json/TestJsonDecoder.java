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
package io.prestosql.decoder.json;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.DecoderTestColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.decoder.util.DecoderTestUtil.checkIsNull;
import static io.prestosql.decoder.util.DecoderTestUtil.checkValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJsonDecoder
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    @Test
    public void testSimple()
            throws Exception
    {
        byte[] json = ByteStreams.toByteArray(TestJsonDecoder.class.getResourceAsStream("/decoder/json/message.json"));

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "source", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", createVarcharType(10), "user/screen_name", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", BIGINT, "id", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BIGINT, "user/statuses_count", null, null, false, false, false);
        DecoderTestColumnHandle column5 = new DecoderTestColumnHandle(4, "column5", BOOLEAN, "user/geo_enabled", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4, column5);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, column1, "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>");
        checkValue(decodedRow, column2, "EKentuckyN");
        checkValue(decodedRow, column3, 493857959588286460L);
        checkValue(decodedRow, column4, 7630);
        checkValue(decodedRow, column5, true);
    }

    @Test
    public void testNonExistent()
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "very/deep/varchar", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", BIGINT, "no_bigint", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", DOUBLE, "double/is_missing", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BOOLEAN, "hello", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkIsNull(decodedRow, column1);
        checkIsNull(decodedRow, column2);
        checkIsNull(decodedRow, column3);
        checkIsNull(decodedRow, column4);
    }

    @Test
    public void testStringNumber()
    {
        byte[] json = "{\"a_number\":481516,\"a_string\":\"2342\"}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "a_number", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", BIGINT, "a_number", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", createVarcharType(100), "a_string", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BIGINT, "a_string", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(json, null);
        assertTrue(decodedRow.isPresent());

        assertEquals(decodedRow.get().size(), columns.size());

        checkValue(decodedRow.get(), column1, "481516");
        checkValue(decodedRow.get(), column2, 481516);
        checkValue(decodedRow.get(), column3, "2342");
        checkValue(decodedRow.get(), column4, 2342);
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BIGINT, null);
        singleColumnDecoder(INTEGER, null);
        singleColumnDecoder(SMALLINT, null);
        singleColumnDecoder(TINYINT, null);
        singleColumnDecoder(BOOLEAN, null);
        singleColumnDecoder(DOUBLE, null);
        singleColumnDecoder(createUnboundedVarcharType(), null);
        singleColumnDecoder(createVarcharType(100), null);

        for (String dataFormat : ImmutableSet.of("iso8601", "custom-date-time", "rfc2822")) {
            singleColumnDecoder(DATE, dataFormat);
            singleColumnDecoder(TIME, dataFormat);
            singleColumnDecoder(TIME_WITH_TIME_ZONE, dataFormat);
            singleColumnDecoder(TIMESTAMP, dataFormat);
            singleColumnDecoder(TIMESTAMP_WITH_TIME_ZONE, dataFormat);
        }

        for (String dataFormat : ImmutableSet.of("seconds-since-epoch", "milliseconds-since-epoch")) {
            singleColumnDecoder(TIME, dataFormat);
            singleColumnDecoder(TIME_WITH_TIME_ZONE, dataFormat);
            singleColumnDecoder(TIMESTAMP, dataFormat);
            singleColumnDecoder(TIMESTAMP_WITH_TIME_ZONE, dataFormat);
        }

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(REAL, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createDecimalType(10, 4), null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(VARBINARY, null));

        // temporal types are not supported for default field decoder
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DATE, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIME, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIME_WITH_TIME_ZONE, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIMESTAMP, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIMESTAMP_WITH_TIME_ZONE, null));

        // non temporal types are not supported by temporal field decoders
        for (String dataFormat : ImmutableSet.of("iso8601", "custom-date-time", "seconds-since-epoch", "milliseconds-since-epoch", "rfc2822")) {
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(BIGINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(INTEGER, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(SMALLINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TINYINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(BOOLEAN, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DOUBLE, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createUnboundedVarcharType(), dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createVarcharType(100), dataFormat));
        }

        // date are not supported by seconds-since-epoch and milliseconds-since-epoch field decoders
        for (String dataFormat : ImmutableSet.of("seconds-since-epoch", "milliseconds-since-epoch")) {
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DATE, dataFormat));
        }
    }

    private void assertUnsupportedColumnTypeException(ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("unsupported column type .* for column .*");
    }

    @Test
    public void testDataFormatValidation()
    {
        for (Type type : asList(TIMESTAMP, DOUBLE)) {
            assertThatThrownBy(() -> singleColumnDecoder(type, "wrong_format"))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage("unknown data format 'wrong_format' used for column 'some_column'");
        }
    }

    private void singleColumnDecoder(Type columnType, String dataFormat)
    {
        singleColumnDecoder(columnType, "mappedField", dataFormat);
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat)
    {
        String formatHint = "custom-date-time".equals(dataFormat) ? "MM/yyyy/dd H:m:s" : null;
        DECODER_FACTORY.create(emptyMap(), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, false, false, false)));
    }
}
