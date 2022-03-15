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
package com.facebook.presto.execution;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.metadata.ConnectorMetadataUpdateHandleSerde;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.server.thrift.Any;
import com.facebook.presto.spi.ConnectorHandleSerde;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadataUpdateHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTaskWithConnectorTypeSerde
{
    private static final ThriftCodecManager COMPILER_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager REFLECTION_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private static HandleResolver handleResolver;

    @BeforeMethod
    public void setUp()
    {
        handleResolver = getHandleResolver();
    }

    private HandleResolver getHandleResolver()
    {
        HandleResolver handleResolver = new HandleResolver();
        //Register Connector
        handleResolver.addConnectorName("test", new TestingHandleResolver());
        return handleResolver;
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_CODEC_MANAGER, COMPILER_CODEC_MANAGER},
                {COMPILER_CODEC_MANAGER, REFLECTION_CODEC_MANAGER},
                {REFLECTION_CODEC_MANAGER, COMPILER_CODEC_MANAGER},
                {REFLECTION_CODEC_MANAGER, REFLECTION_CODEC_MANAGER}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodecManager readCodecManager, ThriftCodecManager writeCodecManager)
            throws Exception
    {
        TaskWithConnectorType taskDummy = getRoundTripSerialize(readCodecManager, writeCodecManager, TBinaryProtocol::new);
        assertSerde(taskDummy, readCodecManager);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodecManager readCodecManager, ThriftCodecManager writeCodecManager)
            throws Exception
    {
        TaskWithConnectorType taskDummy = getRoundTripSerialize(readCodecManager, writeCodecManager, TBinaryProtocol::new);
        assertSerde(taskDummy, readCodecManager);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodecManager readCodecManager, ThriftCodecManager writeCodecManager)
            throws Exception
    {
        TaskWithConnectorType taskDummy = getRoundTripSerialize(readCodecManager, writeCodecManager, TBinaryProtocol::new);
        assertSerde(taskDummy, readCodecManager);
    }

    private void assertSerde(TaskWithConnectorType taskWithConnectorType, ThriftCodecManager readCodecManager)
    {
        assertEquals(100, taskWithConnectorType.getVal());
        Any connectorMetadataUpdateHandleAny = taskWithConnectorType.getConnectorMetadataUpdateHandleAny();
        TestingMetadataUpdateHandle connectorMetadataUpdateHandle = (TestingMetadataUpdateHandle) getConnectorSerde(readCodecManager)
                .deSerialize(handleResolver.getMetadataUpdateHandleClass(connectorMetadataUpdateHandleAny.getId()),
                connectorMetadataUpdateHandleAny.getBytes());
        assertEquals(200, connectorMetadataUpdateHandle.getVal());
    }

    private TaskWithConnectorType getRoundTripSerialize(ThriftCodecManager readCodecManager, ThriftCodecManager writeCodecManager, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        ThriftCodec<TaskWithConnectorType> writeCodec = writeCodecManager.getCodec(TaskWithConnectorType.class);
        writeCodec.write(getTaskDummy(writeCodecManager), protocol);
        ThriftCodec<TaskWithConnectorType> readCodec = readCodecManager.getCodec(TaskWithConnectorType.class);
        return readCodec.read(protocol);
    }

    private TaskWithConnectorType getTaskDummy(ThriftCodecManager thriftCodecManager)
    {
        //Connector specific type
        TestingMetadataUpdateHandle metadataUpdateHandle = new TestingMetadataUpdateHandle(200);
        byte[] serialized = getConnectorSerde(thriftCodecManager).serialize(metadataUpdateHandle);
        String id = handleResolver.getId(metadataUpdateHandle);
        Any any = new Any(id, serialized);
        return new TaskWithConnectorType(100, any);
    }

    private static ConnectorHandleSerde<ConnectorMetadataUpdateHandle> getConnectorSerde(ThriftCodecManager thriftCodecManager)
    {
        return new ConnectorMetadataUpdateHandleSerde<>(thriftCodecManager, Protocol.BINARY);
    }
}
