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
package com.facebook.presto.decoder.protobuf;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestProtobufDecoder
{
    private static final ProtobufFieldDecoder DEFAULT_FIELD_DECODER = new ProtobufFieldDecoder();

    private static Map<DecoderColumnHandle, FieldDecoder<?>> buildMap(List<DecoderColumnHandle> columns)
    {
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> map = ImmutableMap.builder();
        for (DecoderColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple()
    {
        String className = "com.facebook.presto.decoder.protobuf.MessageSchema$Message";
        MessageSchema.Message.User user = MessageSchema.Message.User.newBuilder()
                                            .setId(98247748L)
                                            .setIdStr("98247748")
                                            .setScreenName("EKentuckyNews")
                                            .setGeoEnabled(true)
                                            .setStatusesCount(7630)
                                            .build();
        MessageSchema.Message message = MessageSchema.Message.newBuilder()
                                            .setCreatedAt("Mon Jul 28 20:38:07 +0000 2014")
                                            .setId(493857959588286460L)
                                            .setIdStr("493857959588286465")
                                            .setSource("<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>")
                                            .setUser(user)
                                            .build();
        ProtobufRowDecoder rowDecoder = new ProtobufRowDecoder();
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", createVarcharType(100), "source", className, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(10), "user/screen_name", className, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "id", className, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "user/statuses_count", className, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", BooleanType.BOOLEAN, "user/geo_enabled", className, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(message.toByteArray(), null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>");
        checkValue(providers, row2, "EKentuckyN");
        checkValue(providers, row3, 493857959588286460L);
        checkValue(providers, row4, 7630);
        checkValue(providers, row5, true);
    }
}
