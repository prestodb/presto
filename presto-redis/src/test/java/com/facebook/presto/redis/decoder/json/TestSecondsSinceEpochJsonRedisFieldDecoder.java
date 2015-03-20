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
package com.facebook.presto.redis.decoder.json;

import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.redis.RedisFieldValueProvider;
import com.facebook.presto.redis.decoder.RedisFieldDecoder;
import com.facebook.presto.redis.decoder.util.DecoderTestUtil;
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

import static com.facebook.presto.redis.decoder.RedisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.facebook.presto.redis.decoder.json.SecondsSinceEpochJsonRedisFieldDecoder.FORMATTER;
//import static com.facebook.presto.redis.decoder.util.DecoderTestUtil.checkValue;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestSecondsSinceEpochJsonRedisFieldDecoder
{
    private static final Map<String, JsonRedisFieldDecoder> DECODERS = ImmutableMap.of(DEFAULT_FIELD_DECODER_NAME, new JsonRedisFieldDecoder(),
            SecondsSinceEpochJsonRedisFieldDecoder.NAME, new SecondsSinceEpochJsonRedisFieldDecoder());

    private static final ObjectMapperProvider PROVIDER = new ObjectMapperProvider();

    private static Map<RedisColumnHandle, RedisFieldDecoder<?>> buildMap(List<RedisColumnHandle> columns)
    {
        ImmutableMap.Builder<RedisColumnHandle, RedisFieldDecoder<?>> map = ImmutableMap.builder();
        for (RedisColumnHandle column : columns) {
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

        JsonRedisRowDecoder rowDecoder = new JsonRedisRowDecoder(PROVIDER.get());
        RedisColumnHandle row1 = new RedisColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        RedisColumnHandle row2 = new RedisColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        RedisColumnHandle row3 = new RedisColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);
        RedisColumnHandle row4 = new RedisColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);

        RedisColumnHandle row5 = new RedisColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);
        RedisColumnHandle row6 = new RedisColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);

        List<RedisColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<RedisFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        DecoderTestUtil.checkValue(providers, row1, now);
        DecoderTestUtil.checkValue(providers, row2, Long.toString(now));

        // number parsed as number --> return as time stamp (millis)
        DecoderTestUtil.checkValue(providers, row3, now * 1000);
        // string parsed as number --> parse text, convert to timestamp
        DecoderTestUtil.checkValue(providers, row4, now * 1000);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        DecoderTestUtil.checkValue(providers, row5, nowString);

        // string parsed as string --> parse text, convert to timestamp, turn into string
        DecoderTestUtil.checkValue(providers, row6, nowString);
    }

    @Test
    public void testNullValues()
            throws Exception
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        JsonRedisRowDecoder rowDecoder = new JsonRedisRowDecoder(PROVIDER.get());
        RedisColumnHandle row1 = new RedisColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);
        RedisColumnHandle row2 = new RedisColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", DEFAULT_FIELD_DECODER_NAME, null, false, false, false);

        RedisColumnHandle row3 = new RedisColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);
        RedisColumnHandle row4 = new RedisColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);

        RedisColumnHandle row5 = new RedisColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);
        RedisColumnHandle row6 = new RedisColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", SecondsSinceEpochJsonRedisFieldDecoder.NAME, null, false, false, false);

        List<RedisColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<RedisFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        DecoderTestUtil.checkIsNull(providers, row1);
        DecoderTestUtil.checkIsNull(providers, row2);
        DecoderTestUtil.checkIsNull(providers, row3);
        DecoderTestUtil.checkIsNull(providers, row4);
        DecoderTestUtil.checkIsNull(providers, row5);
        DecoderTestUtil.checkIsNull(providers, row6);
    }
}
