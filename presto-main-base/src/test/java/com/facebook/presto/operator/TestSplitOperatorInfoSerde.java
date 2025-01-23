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
package com.facebook.presto.operator;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSplitOperatorInfoSerde
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<SplitOperatorInfo> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(SplitOperatorInfo.class);
    private static final ThriftCodec<SplitOperatorInfo> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(SplitOperatorInfo.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<SplitOperatorInfo> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(SplitOperatorInfo.class);
    private static final ThriftCodec<SplitOperatorInfo> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(SplitOperatorInfo.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private SplitOperatorInfo splitOperatorInfo;

    @BeforeMethod
    public void setUp()
    {
        splitOperatorInfo = getSplitOperatorInfo();
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<SplitOperatorInfo> readCodec, ThriftCodec<SplitOperatorInfo> writeCodec)
            throws Exception
    {
        SplitOperatorInfo splitOperatorInfo = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(splitOperatorInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<SplitOperatorInfo> readCodec, ThriftCodec<SplitOperatorInfo> writeCodec)
            throws Exception
    {
        SplitOperatorInfo splitOperatorInfo = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(splitOperatorInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<SplitOperatorInfo> readCodec, ThriftCodec<SplitOperatorInfo> writeCodec)
            throws Exception
    {
        SplitOperatorInfo splitOperatorInfo = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(splitOperatorInfo);
    }

    private SplitOperatorInfo getRoundTripSerialize(ThriftCodec<SplitOperatorInfo> readCodec, ThriftCodec<SplitOperatorInfo> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(splitOperatorInfo, protocol);
        return readCodec.read(protocol);
    }

    private void assertSerde(SplitOperatorInfo splitOperatorInfo)
    {
        assertEquals(splitOperatorInfo.getSplitInfoMap(), getInfoMap());
    }

    private SplitOperatorInfo getSplitOperatorInfo()
    {
        Map<String, String> infoMap = getInfoMap();

        return new SplitOperatorInfo(infoMap);
    }

    private Map<String, String> getInfoMap()
    {
        return ImmutableMap.<String, String>builder()
                .put("path", "path")
                .put("start", Long.toString(100))
                .put("length", Long.toString(200))
                .put("fileSize", Long.toString(300))
                .build();
    }
}
