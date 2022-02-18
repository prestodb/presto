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
package com.facebook.presto.server;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestThreadResource
{
    private static final ThriftCatalog COMMON_CATALOG = new ThriftCatalog();
    private static final DataSizeToBytesThriftCodec DATA_SIZE_CODEC = new DataSizeToBytesThriftCodec(COMMON_CATALOG);
    private static final JodaDateTimeToEpochMillisThriftCodec DATE_TIME_CODEC = new JodaDateTimeToEpochMillisThriftCodec(COMMON_CATALOG);
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ThreadResource.Info> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(ThreadResource.Info.class);
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ThreadResource.Info> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(ThreadResource.Info.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ThreadResource.Info> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(ThreadResource.Info.class);
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ThreadResource.Info> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(ThreadResource.Info.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

    // Dummy values for fake StackLine
    private static final String FAKE_FILE_1 = "/fake/com/facebook/presto/server/Fake1.java";
    private static final String FAKE_FILE_2 = "/fake/com/facebook/presto/server/Fake2.java";
    private static final String FAKE_FILE_3 = "/fake/com/facebook/presto/server/Fake3.java";
    private static final int FAKE_LINE_1 = 1;
    private static final int FAKE_LINE_2 = 2;
    private static final int FAKE_LINE_3 = 3;
    private static final String FAKE_CLASSNAME_1 = "com.facebook.presto.server.Fake1";
    private static final String FAKE_CLASSNAME_2 = "com.facebook.presto.server.Fake2";
    private static final String FAKE_CLASSNAME_3 = "com.facebook.presto.server.Fake3";
    private static final String FAKE_METHOD_1 = "fake1";
    private static final String FAKE_METHOD_2 = "fake2";
    private static final String FAKE_METHOD_3 = "fake3";

    // Dummy values for fake Info
    private static final long FAKE_ID = 1234L;
    private static final String FAKE_NAME = "Thread-1";
    private static final String FAKE_STATE = "blocked";
    private static final Long FAKE_LOCK_OWNER_ID = 1235L;

    private ThreadResource.Info info;

    @BeforeMethod
    public void setUp()
    {
        List<ThreadResource.StackLine> stackLines = new ArrayList<>();
        stackLines.add(new ThreadResource.StackLine(FAKE_FILE_1, FAKE_LINE_1, FAKE_CLASSNAME_1, FAKE_METHOD_1));
        stackLines.add(new ThreadResource.StackLine(FAKE_FILE_2, FAKE_LINE_2, FAKE_CLASSNAME_2, FAKE_METHOD_2));
        stackLines.add(new ThreadResource.StackLine(FAKE_FILE_3, FAKE_LINE_3, FAKE_CLASSNAME_3, FAKE_METHOD_3));

        info = new ThreadResource.Info(FAKE_ID, FAKE_NAME, FAKE_STATE, FAKE_LOCK_OWNER_ID, stackLines);
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {{COMPILER_READ_CODEC, COMPILER_WRITE_CODEC}, {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC}, {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}};
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<ThreadResource.Info> readCodec, ThriftCodec<ThreadResource.Info> writeCodec)
            throws Exception
    {
        ThreadResource.Info serializedInfo = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertInfo(serializedInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<ThreadResource.Info> readCodec, ThriftCodec<ThreadResource.Info> writeCodec)
            throws Exception
    {
        ThreadResource.Info serializedInfo = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertInfo(serializedInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<ThreadResource.Info> readCodec, ThriftCodec<ThreadResource.Info> writeCodec)
            throws Exception
    {
        ThreadResource.Info serializedInfo = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertInfo(serializedInfo);
    }

    private void assertStackLines(List<ThreadResource.StackLine> stackLines)
    {
        assertEquals(stackLines.get(0).getFile(), FAKE_FILE_1);
        assertEquals(stackLines.get(0).getLine(), FAKE_LINE_1);
        assertEquals(stackLines.get(0).getClassName(), FAKE_CLASSNAME_1);
        assertEquals(stackLines.get(0).getMethod(), FAKE_METHOD_1);

        assertEquals(stackLines.get(1).getFile(), FAKE_FILE_2);
        assertEquals(stackLines.get(1).getLine(), FAKE_LINE_2);
        assertEquals(stackLines.get(1).getClassName(), FAKE_CLASSNAME_2);
        assertEquals(stackLines.get(1).getMethod(), FAKE_METHOD_2);

        assertEquals(stackLines.get(2).getFile(), FAKE_FILE_3);
        assertEquals(stackLines.get(2).getLine(), FAKE_LINE_3);
        assertEquals(stackLines.get(2).getClassName(), FAKE_CLASSNAME_3);
        assertEquals(stackLines.get(2).getMethod(), FAKE_METHOD_3);
    }

    private void assertInfo(ThreadResource.Info actualInfo)
    {
        assertEquals(actualInfo.getId(), FAKE_ID);
        assertEquals(actualInfo.getName(), FAKE_NAME);
        assertEquals(actualInfo.getState(), FAKE_STATE);
        assertEquals(actualInfo.getLockOwnerId(), FAKE_LOCK_OWNER_ID);
        assertStackLines(actualInfo.getStackTrace());
    }

    private ThreadResource.Info getRoundTripSerialize(ThriftCodec<ThreadResource.Info> readCodec, ThriftCodec<ThreadResource.Info> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(info, protocol);
        return readCodec.read(protocol);
    }
}
