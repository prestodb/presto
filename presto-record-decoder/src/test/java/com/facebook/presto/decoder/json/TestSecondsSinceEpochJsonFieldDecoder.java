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
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.facebook.presto.decoder.json.SecondsSinceEpochJsonFieldDecoder.FORMATTER;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestSecondsSinceEpochJsonFieldDecoder
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    @Test
    public void testBasicFormatting()
    {
        long now = System.currentTimeMillis() / 1000; // SecondsSinceEpoch is second granularity
        String nowString = FORMATTER.print(now * 1000);

        byte[] json = format("{\"a_number\":%d,\"a_string\":\"%d\"}", now, now).getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle column5 = new DecoderTestColumnHandle(4, "column5", createVarcharType(100), "a_number", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle column6 = new DecoderTestColumnHandle(5, "column6", createVarcharType(100), "a_string", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4, column5, column6);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        // sanity checks
        checkValue(decodedRow, column1, now);
        checkValue(decodedRow, column2, Long.toString(now));

        // number parsed as number --> return as time stamp (millis)
        checkValue(decodedRow, column3, now * 1000);
        // string parsed as number --> parse text, convert to timestamp
        checkValue(decodedRow, column4, now * 1000);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(decodedRow, column5, nowString);

        // string parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(decodedRow, column6, nowString);
    }

    @Test
    public void testNullValues()
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle column5 = new DecoderTestColumnHandle(4, "column5", createVarcharType(100), "a_number", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle column6 = new DecoderTestColumnHandle(5, "column6", createVarcharType(100), "a_string", SecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4, column5, column6);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        // sanity checks
        checkIsNull(decodedRow, column1);
        checkIsNull(decodedRow, column2);
        checkIsNull(decodedRow, column3);
        checkIsNull(decodedRow, column4);
        checkIsNull(decodedRow, column5);
        checkIsNull(decodedRow, column6);
    }
}
