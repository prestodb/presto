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
package com.facebook.presto.decoder.thrift;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.thrift.tweep.Location;
import com.facebook.presto.decoder.thrift.tweep.Tweet;
import com.facebook.presto.decoder.thrift.tweep.TweetType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestThriftDecoder
{
    private static final ThriftFieldDecoder DEFAULT_FIELD_DECODER = new ThriftFieldDecoder();

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
            throws Exception
    {
        Tweet tweet = new Tweet(1, "newUser", "hello world")
                .setLoc(new Location(1234, 5678))
                .setAge((short) 26)
                .setB((byte) 10)
                .setIsDeleted(false)
                .setTweetType(TweetType.REPLY)
                .setFullId(1234567)
                .setPic("abc".getBytes())
                .setAttr(ImmutableMap.of("a", "a"));

        ThriftRowDecoder rowDecoder = new ThriftRowDecoder();

        // schema
        DecoderTestColumnHandle col1 = new DecoderTestColumnHandle("", 1, "user_id", IntegerType.INTEGER, "1", "thrift", null, false, false, false);
        DecoderTestColumnHandle col2 = new DecoderTestColumnHandle("", 2, "username", createVarcharType(100), "2", "thrift", null, false, false, false);
        DecoderTestColumnHandle col3 = new DecoderTestColumnHandle("", 3, "text", createVarcharType(100), "3", "thrift", null, false, false, false);
        DecoderTestColumnHandle col4 = new DecoderTestColumnHandle("", 4, "loc.latitude", DoubleType.DOUBLE, "4/1", "thrift", null, false, false, false);
        DecoderTestColumnHandle col5 = new DecoderTestColumnHandle("", 5, "loc.longitude", DoubleType.DOUBLE, "4/2", "thrift", null, false, false, false);
        DecoderTestColumnHandle col6 = new DecoderTestColumnHandle("", 6, "tweet_type", BigintType.BIGINT, "5", "thrift", null, false, false, false);
        DecoderTestColumnHandle col7 = new DecoderTestColumnHandle("", 7, "is_deleted", BooleanType.BOOLEAN, "6", "thrift", null, false, false, false);
        DecoderTestColumnHandle col8 = new DecoderTestColumnHandle("", 8, "b", TinyintType.TINYINT, "7", "thrift", null, false, false, false);
        DecoderTestColumnHandle col9 = new DecoderTestColumnHandle("", 9, "age", SmallintType.SMALLINT, "8", "thrift", null, false, false, false);
        DecoderTestColumnHandle col10 = new DecoderTestColumnHandle("", 10, "full_id", BigintType.BIGINT, "9", "thrift", null, false, false, false);
        DecoderTestColumnHandle col11 = new DecoderTestColumnHandle("", 11, "pic", VarbinaryType.VARBINARY, "10", "thrift", null, false, false, false);
        DecoderTestColumnHandle col12 = new DecoderTestColumnHandle("", 12, "language", createVarcharType(100), "16", "thrift", null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12);
        Set<FieldValueProvider> providers = new HashSet<>();

        TMemoryBuffer transport = new TMemoryBuffer(4096);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        tweet.write(protocol);

        boolean corrupt = rowDecoder.decodeRow(transport.getArray(), null, providers, columns, buildMap(columns));
        assertFalse(corrupt);
        assertEquals(providers.size(), columns.size());

        checkValue(providers, col1, 1);
        checkValue(providers, col2, "newUser");
        checkValue(providers, col3, "hello world");
        checkValue(providers, col4, 1234);
        checkValue(providers, col5, 5678);
        checkValue(providers, col6, TweetType.REPLY.getValue());
        checkValue(providers, col7, false);
        checkValue(providers, col8, 10);
        checkValue(providers, col9, 26);
        checkValue(providers, col10, 1234567);
        checkValue(providers, col11, "abc");
        checkValue(providers, col12, "english");
    }
}
