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
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.util.Failures;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static org.testng.Assert.assertEquals;

public class TestThriftTaskStatus
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskStatus> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodec<TaskStatus> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<TaskStatus> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodec<TaskStatus> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    public static final long TASK_INSTANCE_ID_LEAST_SIGNIFICANT_BITS = 123L;
    public static final long TASK_INSTANCE_ID_MOST_SIGNIFICANT_BITS = 456L;
    public static final long VERSION = 789L;
    public static final TaskState RUNNING = TaskState.RUNNING;
    public static final URI SELF_URI = java.net.URI.create("fake://task/" + "1");
    public static final Set<Lifespan> LIFESPANS = ImmutableSet.of(Lifespan.taskWide(), Lifespan.driverGroup(100));
    public static final int QUEUED_PARTITIONED_DRIVERS = 100;
    public static final int RUNNING_PARTITIONED_DRIVERS = 200;
    public static final double OUTPUT_BUFFER_UTILIZATION = 99.9;
    public static final boolean OUTPUT_BUFFER_OVERUTILIZED = true;
    public static final int PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES = 1024 * 1024;
    public static final int MEMORY_RESERVATION_IN_BYTES = 1024 * 1024 * 1024;
    public static final int SYSTEM_MEMORY_RESERVATION_IN_BYTES = 2 * 1024 * 1024 * 1024;
    public static final int PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES = 42 * 1024 * 1024;
    public static final int FULL_GC_COUNT = 10;
    public static final int FULL_GC_TIME_IN_MILLIS = 1001;
    public static final HostAddress REMOTE_HOST = HostAddress.fromParts("www.fake.invalid", 8080);
    private TaskStatus taskStatus;

    @BeforeMethod
    public void setUp()
    {
        taskStatus = getTaskStatus();
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
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskStatus);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(taskStatus);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(taskStatus);
    }

    private void assertSerde(TaskStatus taskStatus)
    {
        assertEquals(taskStatus.getTaskInstanceIdLeastSignificantBits(), 123L);
        assertEquals(taskStatus.getTaskInstanceIdMostSignificantBits(), 456L);
        assertEquals(taskStatus.getVersion(), 789L);
        assertEquals(taskStatus.getState(), TaskState.RUNNING);
        assertEquals(taskStatus.getSelf(), SELF_URI);
        assertEquals(taskStatus.getCompletedDriverGroups(), LIFESPANS);
        assertEquals(taskStatus.getQueuedPartitionedDrivers(), QUEUED_PARTITIONED_DRIVERS);
        assertEquals(taskStatus.getRunningPartitionedDrivers(), RUNNING_PARTITIONED_DRIVERS);
        assertEquals(taskStatus.getOutputBufferUtilization(), OUTPUT_BUFFER_UTILIZATION);
        assertEquals(taskStatus.isOutputBufferOverutilized(), OUTPUT_BUFFER_OVERUTILIZED);
        assertEquals(taskStatus.getPhysicalWrittenDataSizeInBytes(), PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES);
        assertEquals(taskStatus.getSystemMemoryReservationInBytes(), SYSTEM_MEMORY_RESERVATION_IN_BYTES);
        assertEquals(taskStatus.getPeakNodeTotalMemoryReservationInBytes(), PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES);
        assertEquals(taskStatus.getFullGcCount(), FULL_GC_COUNT);
        assertEquals(taskStatus.getFullGcTimeInMillis(), FULL_GC_TIME_IN_MILLIS);

        List<ExecutionFailureInfo> failures = taskStatus.getFailures();
        assertEquals(failures.size(), 3);

        ExecutionFailureInfo firstFailure = failures.get(0);
        assertEquals(firstFailure.getType(), IOException.class.getName());
        assertEquals(firstFailure.getMessage(), "Remote call timed out");
        assertEquals(firstFailure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        List<ExecutionFailureInfo> suppressedFailures = firstFailure.getSuppressed();
        assertEquals(suppressedFailures.size(), 1);
        ExecutionFailureInfo suppressedFailure = suppressedFailures.get(0);
        assertEquals(suppressedFailure.getType(), IOException.class.getName());
        assertEquals(suppressedFailure.getMessage(), "Thrift call timed out");
        assertEquals(suppressedFailure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());

        ExecutionFailureInfo secondFailure = failures.get(1);
        assertEquals(secondFailure.getType(), PrestoTransportException.class.getName());
        assertEquals(secondFailure.getMessage(), "Too many requests failed");
        assertEquals(secondFailure.getRemoteHost(), REMOTE_HOST);
        assertEquals(secondFailure.getErrorCode(), TOO_MANY_REQUESTS_FAILED.toErrorCode());
        ExecutionFailureInfo cause = secondFailure.getCause();
        assertEquals(cause.getType(), PrestoException.class.getName());
        assertEquals(cause.getMessage(), "Remote Task Error");
        assertEquals(cause.getErrorCode(), REMOTE_TASK_ERROR.toErrorCode());

        ExecutionFailureInfo thirdFailure = failures.get(2);
        assertEquals(thirdFailure.getType(), ParsingException.class.getName());
        assertEquals(thirdFailure.getErrorCode(), SYNTAX_ERROR.toErrorCode());
        assertEquals(thirdFailure.getErrorLocation().getLineNumber(), 100);
        assertEquals(thirdFailure.getErrorLocation().getColumnNumber(), 2);
    }

    private TaskStatus getRoundTripSerialize(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskStatus, protocol);
        return readCodec.read(protocol);
    }

    private TaskStatus getTaskStatus()
    {
        List<ExecutionFailureInfo> executionFailureInfos = getExecutionFailureInfos();
        return new TaskStatus(
                TASK_INSTANCE_ID_LEAST_SIGNIFICANT_BITS,
                TASK_INSTANCE_ID_MOST_SIGNIFICANT_BITS,
                VERSION,
                RUNNING,
                SELF_URI,
                LIFESPANS,
                executionFailureInfos,
                QUEUED_PARTITIONED_DRIVERS,
                RUNNING_PARTITIONED_DRIVERS,
                OUTPUT_BUFFER_UTILIZATION,
                OUTPUT_BUFFER_OVERUTILIZED,
                PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES,
                MEMORY_RESERVATION_IN_BYTES,
                SYSTEM_MEMORY_RESERVATION_IN_BYTES,
                PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES,
                FULL_GC_COUNT,
                FULL_GC_TIME_IN_MILLIS);
    }

    private List<ExecutionFailureInfo> getExecutionFailureInfos()
    {
        IOException ioException = new IOException("Remote call timed out");
        ioException.addSuppressed(new IOException("Thrift call timed out"));
        PrestoTransportException prestoTransportException = new PrestoTransportException(TOO_MANY_REQUESTS_FAILED,
                REMOTE_HOST,
                "Too many requests failed",
                new PrestoException(REMOTE_TASK_ERROR, "Remote Task Error"));
        ParsingException parsingException = new ParsingException("Parsing Exception", new NodeLocation(100, 1));
        return Failures.toFailures(ImmutableList.of(ioException, prestoTransportException, parsingException));
    }
}
