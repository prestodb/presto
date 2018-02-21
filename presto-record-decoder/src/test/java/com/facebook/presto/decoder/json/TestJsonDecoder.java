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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.emptyMap;
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
}
