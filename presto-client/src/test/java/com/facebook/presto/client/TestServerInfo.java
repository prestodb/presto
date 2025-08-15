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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import io.airlift.units.Duration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestServerInfo
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);
    private static final ThriftCatalog COMMON_CATALOG = new ThriftCatalog();
    private static final DurationToMillisThriftCodec DURATION_CODEC = new DurationToMillisThriftCodec(COMMON_CATALOG);
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), DURATION_CODEC);
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), DURATION_CODEC);
    private static final ThriftCodec<ServerInfo> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(ServerInfo.class);
    private static final ThriftCodec<ServerInfo> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(ServerInfo.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), DURATION_CODEC);
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), DURATION_CODEC);
    private static final ThriftCodec<ServerInfo> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(ServerInfo.class);
    private static final ThriftCodec<ServerInfo> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(ServerInfo.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

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

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m"))));
        assertJsonRoundTrip(new ServerInfo(UNKNOWN, "test", true, false, Optional.empty()));
    }

    @Test
    public void testBackwardsCompatible()
    {
        ServerInfo newServerInfo = new ServerInfo(UNKNOWN, "test", true, false, Optional.empty());
        ServerInfo legacyServerInfo = SERVER_INFO_CODEC.fromJson("{\"nodeVersion\":{\"version\":\"<unknown>\"},\"environment\":\"test\",\"coordinator\":true}");
        assertEquals(newServerInfo, legacyServerInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<ServerInfo> readCodec, ThriftCodec<ServerInfo> writeCodec)
            throws Exception
    {
        ServerInfo serverInfo = getServerInfo();
        ServerInfo roundTripServerInfo = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new, serverInfo);
        assertEquals(serverInfo, roundTripServerInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeCompactProtocol(ThriftCodec<ServerInfo> readCodec, ThriftCodec<ServerInfo> writeCodec)
            throws Exception
    {
        ServerInfo serverInfo = getServerInfo();
        ServerInfo roundTripServerInfo = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new, serverInfo);
        assertEquals(serverInfo, roundTripServerInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeFacebookCompactProtocol(ThriftCodec<ServerInfo> readCodec, ThriftCodec<ServerInfo> writeCodec)
            throws Exception
    {
        ServerInfo serverInfo = getServerInfo();
        ServerInfo roundTripServerInfo = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new, serverInfo);
        assertEquals(serverInfo, roundTripServerInfo);
    }

    private ServerInfo getServerInfo()
    {
        return new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));
    }

    private ServerInfo getRoundTripSerialize(ThriftCodec<ServerInfo> readCodec, ThriftCodec<ServerInfo> writeCodec,
            Function<TTransport, TProtocol> protocolFactory, ServerInfo serverInfo) throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(serverInfo, protocol);
        return readCodec.read(protocol);
    }

    private static void assertJsonRoundTrip(ServerInfo serverInfo)
    {
        String json = SERVER_INFO_CODEC.toJson(serverInfo);
        ServerInfo copy = SERVER_INFO_CODEC.fromJson(json);
        assertEquals(copy, serverInfo);
    }
}
