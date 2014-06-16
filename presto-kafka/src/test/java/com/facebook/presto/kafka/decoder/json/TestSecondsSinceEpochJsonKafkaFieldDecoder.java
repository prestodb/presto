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
package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.kafka.decoder.KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.facebook.presto.kafka.decoder.json.SecondsSinceEpochJsonKafkaFieldDecoder.FORMATTER;
import static com.facebook.presto.kafka.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.kafka.decoder.util.DecoderTestUtil.checkValue;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestSecondsSinceEpochJsonKafkaFieldDecoder
{
    private static final Map<String, JsonKafkaFieldDecoder> DECODERS = ImmutableMap.of(DEFAULT_FIELD_DECODER_NAME, new JsonKafkaFieldDecoder(),
            SecondsSinceEpochJsonKafkaFieldDecoder.NAME, new SecondsSinceEpochJsonKafkaFieldDecoder());

    private static final ObjectMapperProvider PROVIDER = new ObjectMapperProvider();

    private static Map<KafkaColumnHandle, KafkaFieldDecoder<?>> buildMap(List<KafkaColumnHandle> columns)
    {
        ImmutableMap.Builder<KafkaColumnHandle, KafkaFieldDecoder<?>> map = ImmutableMap.builder();
        for (KafkaColumnHandle column : columns) {
            map.put(column, DECODERS.get(column.getDataFormat()));
        }
        return map.build();
    }

    @Test
    public void testBasicFormatting()
            throws Exception
    {
        long now = System.currentTimeMillis() / 1000; // SecondsSinceEpoch is second granularity
        String nowString = FORMATTER.print(now * 1000);

        byte[] json = format("{\"a_number\":%d,\"a_string\":\"%d\"}", now, now).getBytes(StandardCharsets.UTF_8);

        JsonKafkaRowDecoder rowDecoder = new JsonKafkaRowDecoder(PROVIDER.get());
        KafkaColumnHandle row1 = new KafkaColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        KafkaColumnHandle row2 = new KafkaColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        KafkaColumnHandle row3 = new KafkaColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);
        KafkaColumnHandle row4 = new KafkaColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);

        KafkaColumnHandle row5 = new KafkaColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);
        KafkaColumnHandle row6 = new KafkaColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);

        List<KafkaColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<KafkaFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        checkValue(providers, row1, now);
        checkValue(providers, row2, Long.toString(now));

        // number parsed as number --> return as time stamp (millis)
        checkValue(providers, row3, now * 1000);
        // string parsed as number --> parse text, convert to timestamp
        checkValue(providers, row4, now * 1000);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(providers, row5, nowString);

        // string parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(providers, row6, nowString);
    }

    @Test
    public void testNullValues()
            throws Exception
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        JsonKafkaRowDecoder rowDecoder = new JsonKafkaRowDecoder(PROVIDER.get());
        KafkaColumnHandle row1 = new KafkaColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        KafkaColumnHandle row2 = new KafkaColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        KafkaColumnHandle row3 = new KafkaColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);
        KafkaColumnHandle row4 = new KafkaColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);

        KafkaColumnHandle row5 = new KafkaColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);
        KafkaColumnHandle row6 = new KafkaColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", SecondsSinceEpochJsonKafkaFieldDecoder.NAME, null, false, false, false);

        List<KafkaColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<KafkaFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, providers, columns, buildMap(columns));
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
