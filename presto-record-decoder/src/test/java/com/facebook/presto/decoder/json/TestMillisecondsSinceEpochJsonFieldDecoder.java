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
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.decoder.FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMillisecondsSinceEpochJsonFieldDecoder
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    @Test
    public void testBasicFormatting()
    {
        long now = System.currentTimeMillis();
        String nowString = MillisecondsSinceEpochJsonFieldDecoder.FORMATTER.print(now);

        byte[] json = format("{\"a_number\":%d,\"a_string\":\"%d\"}", now, now).getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", createVarcharType(100), "a_number", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", createVarcharType(100), "a_string", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json, null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        // sanity checks
        checkValue(decodedRow, row1, now);
        checkValue(decodedRow, row2, Long.toString(now));

        // number parsed as number --> return as time stamp (millis)
        checkValue(decodedRow, row3, now);
        // string parsed as number --> parse text, convert to timestamp
        checkValue(decodedRow, row4, now);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(decodedRow, row5, nowString);

        // string parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(decodedRow, row6, nowString);
    }

    @Test
    public void testNullValues()
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(100), "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", createVarcharType(100), "a_number", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", createVarcharType(100), "a_string", MillisecondsSinceEpochJsonFieldDecoder.NAME, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(json, null);
        assertTrue(decodedRow.isPresent());

        assertEquals(decodedRow.get().size(), columns.size());

        // sanity checks
        checkIsNull(decodedRow.get(), row1);
        checkIsNull(decodedRow.get(), row2);
        checkIsNull(decodedRow.get(), row3);
        checkIsNull(decodedRow.get(), row4);
        checkIsNull(decodedRow.get(), row5);
        checkIsNull(decodedRow.get(), row6);
    }
}
