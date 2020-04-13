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
package com.facebook.presto.thrift;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.coercion.DefaultJavaCoercions;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.operator.TaskStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.function.Function;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestTaskInfoSerDe
{
    private static final ThriftCodecManager READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));

    @BeforeMethod
    protected void setUp()
    {
        READ_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        WRITE_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
    }

    @Test
    public void testLifeSpan()
            throws Exception
    {
        Lifespan lifespan = Lifespan.taskWide();
        assertEquals(lifespan, getRoundTripSerialize(lifespan, TBinaryProtocol::new));
        assertEquals(lifespan, getRoundTripSerialize(lifespan, TCompactProtocol::new));
        assertEquals(lifespan, getRoundTripSerialize(lifespan, TFacebookCompactProtocol::new));
    }

    @Test
    public void testPageBufferInfo()
            throws Exception
    {
        PageBufferInfo pageBufferInfo = new PageBufferInfo(0, 1, 2, 3, 4);
        assertEquals(pageBufferInfo, getRoundTripSerialize(pageBufferInfo, TBinaryProtocol::new));
        assertEquals(pageBufferInfo, getRoundTripSerialize(pageBufferInfo, TCompactProtocol::new));
        assertEquals(pageBufferInfo, getRoundTripSerialize(pageBufferInfo, TFacebookCompactProtocol::new));
    }

    @Test
    public void testBufferInfo()
            throws Exception
    {
        PageBufferInfo pageBufferInfo = new PageBufferInfo(0, 1, 2, 3, 4);
        BufferInfo bufferInfo = new BufferInfo(new OutputBuffers.OutputBufferId(10), false, 2, 3, pageBufferInfo);
        assertEquals(bufferInfo, getRoundTripSerialize(bufferInfo, TBinaryProtocol::new));
        assertEquals(bufferInfo, getRoundTripSerialize(bufferInfo, TCompactProtocol::new));
        assertEquals(bufferInfo, getRoundTripSerialize(bufferInfo, TFacebookCompactProtocol::new));
    }

    @Test
    public void testOutputBufferInfo()
            throws Exception
    {
        PageBufferInfo pageBufferInfo1 = new PageBufferInfo(0, 1, 2, 3, 4);
        BufferInfo bufferInfo1 = new BufferInfo(new OutputBuffers.OutputBufferId(1), false, 2, 3, pageBufferInfo1);
        PageBufferInfo pageBufferInfo2 = new PageBufferInfo(5, 6, 7, 8, 9);
        BufferInfo bufferInfo2 = new BufferInfo(new OutputBuffers.OutputBufferId(2), false, 2, 3, pageBufferInfo2);
        OutputBufferInfo outputBufferInfo = new OutputBufferInfo("test",
                BufferState.OPEN,
                true,
                false,
                1,
                2,
                3,
                4,
                ImmutableList.of(bufferInfo1, bufferInfo2));
        assertEquals(outputBufferInfo, getRoundTripSerialize(outputBufferInfo, TBinaryProtocol::new));
        assertEquals(outputBufferInfo, getRoundTripSerialize(outputBufferInfo, TCompactProtocol::new));
        assertEquals(outputBufferInfo, getRoundTripSerialize(outputBufferInfo, TFacebookCompactProtocol::new));
    }

    private static TaskStatus getTaskStatus()
    {
        ExecutionFailureInfo executionFailureInfo1 = new ExecutionFailureInfo("test1", "m1", null, ImmutableList.of(), ImmutableList.of(), null, null, null);
        ExecutionFailureInfo executionFailureInfo2 = new ExecutionFailureInfo("test1", "m1", executionFailureInfo1, ImmutableList.of(executionFailureInfo1), ImmutableList.of(), null, null, null);
        return new TaskStatus(
                new TaskId("test", 1, 1, 1),
                "test",
                1,
                TaskState.RUNNING,
                URI.create("fake://task/" + "1"),
                "test",
                ImmutableSet.of(Lifespan.taskWide(), Lifespan.taskWide()),
                ImmutableList.of(executionFailureInfo2),
                0,
                0,
                0.0,
                false,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                0,
                new Duration(0, MILLISECONDS));
    }

    @Test
    public void testTaskInfo()
            throws Exception
    {
        TaskInfo taskInfo = getTaskInfo();

        checkTaskInfo(taskInfo, getRoundTripSerialize(taskInfo, TBinaryProtocol::new));
        checkTaskInfo(taskInfo, getRoundTripSerialize(taskInfo, TCompactProtocol::new));
        checkTaskInfo(taskInfo, getRoundTripSerialize(taskInfo, TFacebookCompactProtocol::new));
    }

    public static TaskInfo getTaskInfo()
    {
        return new TaskInfo(
                getTaskStatus(),
                DateTime.now(),
                new OutputBufferInfo("test", BufferState.OPEN, true, true, 0, 0, 0, 0, ImmutableList.of()),
                ImmutableSet.of(),
                new TaskStats(DateTime.now(), DateTime.now()),
                true);
    }

    private void checkTaskInfo(TaskInfo taskInfo1, TaskInfo taskInfo2)
    {
        assertEquals(taskInfo1.getTaskStatus().getState(), taskInfo2.getTaskStatus().getState());
        assertEquals(taskInfo1.getTaskStatus().getTaskId(), taskInfo2.getTaskStatus().getTaskId());
        assertEquals(taskInfo1.getTaskStatus().getSelf(), taskInfo2.getTaskStatus().getSelf());
        assertEquals(taskInfo1.getTaskStatus().getVersion(), taskInfo2.getTaskStatus().getVersion());
        assertEquals(taskInfo1.getTaskStatus().getTaskInstanceId(), taskInfo2.getTaskStatus().getTaskInstanceId());
        assertEquals(taskInfo1.getTaskStatus().getMemoryReservation(), taskInfo2.getTaskStatus().getMemoryReservation());
        assertEquals(taskInfo1.getTaskStatus().getSystemMemoryReservation(), taskInfo2.getTaskStatus().getSystemMemoryReservation());
        assertEquals(taskInfo1.getTaskStatus().getFullGcTimeString(), taskInfo2.getTaskStatus().getFullGcTimeString());
        assertEquals(taskInfo1.getTaskStatus().getFailures().size(), taskInfo2.getTaskStatus().getFailures().size());
        assertEquals(taskInfo1.getStatsJson(), taskInfo2.getStatsJson());
        assertEquals(taskInfo1.getNoMoreSplits(), taskInfo2.getNoMoreSplits());
        assertEquals(taskInfo1.getLastHeartbeatString(), taskInfo2.getLastHeartbeatString());
    }

    public static <T> T getRoundTripSerialize(T value, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCodec<T> readCodec = (ThriftCodec<T>) READ_CODEC_MANAGER.getCodec(value.getClass());
        ThriftCodec<T> writeCodec = (ThriftCodec<T>) WRITE_CODEC_MANAGER.getCodec(value.getClass());

        return getRoundTripSerialize(readCodec, writeCodec, value, protocolFactory);
    }

    private static <T> T getRoundTripSerialize(ThriftCodec<T> readCodec, ThriftCodec<T> writeCodec, T structInstance, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        Class<T> structClass = (Class<T>) structInstance.getClass();
        return getRoundTripSerialize(readCodec, writeCodec, structClass, structInstance, protocolFactory);
    }

    private static <T> T getRoundTripSerialize(ThriftCodec<T> readCodec, ThriftCodec<T> writeCodec, Type structType, T structInstance, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCatalog readCatalog = READ_CODEC_MANAGER.getCatalog();
        ThriftStructMetadata readMetadata = readCatalog.getThriftStructMetadata(structType);
        assertNotNull(readMetadata);

        ThriftCatalog writeCatalog = WRITE_CODEC_MANAGER.getCatalog();
        ThriftStructMetadata writeMetadata = writeCatalog.getThriftStructMetadata(structType);
        assertNotNull(writeMetadata);

        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(structInstance, protocol);

        T copy = readCodec.read(protocol);
        assertNotNull(copy);
        return copy;
    }
}
