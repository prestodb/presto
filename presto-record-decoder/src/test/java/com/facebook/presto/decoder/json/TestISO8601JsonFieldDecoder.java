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
package com.facebook.presto.decoder.json;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestISO8601JsonFieldDecoder
{
    private static final Map<String, JsonFieldDecoder> DECODERS = ImmutableMap.of(DEFAULT_FIELD_DECODER_NAME, new JsonFieldDecoder(),
            ISO8601JsonFieldDecoder.NAME, new ISO8601JsonFieldDecoder());

    private static final ObjectMapperProvider PROVIDER = new ObjectMapperProvider();

    private static final DateTimeFormatter PRINTER = ISODateTimeFormat.dateTime().withLocale(Locale.ENGLISH).withZoneUTC();

    private static Map<DecoderColumnHandle, FieldDecoder<?>> buildMap(List<DecoderColumnHandle> columns)
    {
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> map = ImmutableMap.builder();
        for (DecoderColumnHandle column : columns) {
            map.put(column, DECODERS.get(column.getDataFormat()));
        }
        return map.build();
    }

    @Test
    public void testBasicFormatting()
            throws Exception
    {
        long now = System.currentTimeMillis();
        String nowString = PRINTER.print(now);

        byte[] json = format("{\"a_number\":%d,\"a_string\":\"%s\"}", now, nowString).getBytes(StandardCharsets.UTF_8);

        JsonRowDecoder rowDecoder = new JsonRowDecoder(PROVIDER.get());
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", ISO8601JsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", ISO8601JsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", createVarcharType(100), "a_number", ISO8601JsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", createVarcharType(100), "a_string", ISO8601JsonFieldDecoder.NAME, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        checkValue(providers, row1, now);
        checkValue(providers, row2, nowString);

        // number parsed as number --> as is
        checkValue(providers, row3, now);
        // string parsed as number --> parse text, convert to timestamp
        checkValue(providers, row4, now);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(providers, row5, Long.toString(now));

        // string parsed as string --> as is
        checkValue(providers, row6, nowString);
    }

    @Test
    public void testNullValues()
            throws Exception
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        JsonRowDecoder rowDecoder = new JsonRowDecoder(PROVIDER.get());
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", ISO8601JsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", ISO8601JsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", createVarcharType(100), "a_number", ISO8601JsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", createVarcharType(100), "a_string", ISO8601JsonFieldDecoder.NAME, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        checkIsNull(providers, row1);
        checkIsNull(providers, row2);
        checkIsNull(providers, row3);
        checkIsNull(providers, row4);
        checkIsNull(providers, row5);
        checkIsNull(providers, row6);
    }
}
