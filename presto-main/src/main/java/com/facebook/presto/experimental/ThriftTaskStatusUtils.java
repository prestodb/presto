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
package com.facebook.presto.experimental;

import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.experimental.auto_gen.ThriftTaskState;
import com.facebook.presto.experimental.auto_gen.ThriftTaskStatus;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.ThriftTaskStateUtils.fromTaskState;
import static com.facebook.presto.experimental.ThriftTaskStateUtils.toTaskState;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;

public class ThriftTaskStatusUtils
{
    /**
     * The first valid version that will be returned for a remote task.
     */
    public static final long STARTING_VERSION = 1;

    /**
     * A value lower than {@link #STARTING_VERSION}. This value can be used to
     * create an initial local task that is always older than any remote task.
     */
    private static final long MIN_VERSION = 0;

    /**
     * A value larger than any valid value. This value can be used to create
     * a final local task that is always newer than any remote task.
     */
    private static final long MAX_VERSION = Long.MAX_VALUE;

    private ThriftTaskStatusUtils() {}

    public static ThriftTaskStatus initialTaskStatus(URI location)
    {
        return new ThriftTaskStatus(
                0L,
                0L,
                MIN_VERSION,
                ThriftTaskState.PLANNED,
                location.toString(),
                ImmutableSet.of(),
                ImmutableList.of(),
                0,
                0,
                0.0,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0L,
                0L);
    }

    public static ThriftTaskStatus fromTaskStatus(TaskStatus taskStatus)
    {
        return new ThriftTaskStatus(
                taskStatus.getTaskInstanceIdLeastSignificantBits(),
                taskStatus.getTaskInstanceIdMostSignificantBits(),
                taskStatus.getVersion(),
                fromTaskState(taskStatus.getState()),
                taskStatus.getSelf().toString(),
                taskStatus.getCompletedDriverGroups().stream().map(ThriftLifespanUtils::fromLifespan).collect(Collectors.toSet()),
                taskStatus.getFailures().stream().map(ThriftExecutionFailureInfoUtils::fromExecutionFailureInfo).collect(Collectors.toList()),
                taskStatus.getQueuedPartitionedDrivers(),
                taskStatus.getRunningPartitionedDrivers(),
                taskStatus.getOutputBufferUtilization(),
                taskStatus.isOutputBufferOverutilized(),
                taskStatus.getPhysicalWrittenDataSizeInBytes(),
                taskStatus.getMemoryReservationInBytes(),
                taskStatus.getSystemMemoryReservationInBytes(),
                taskStatus.getPeakNodeTotalMemoryReservationInBytes(),
                taskStatus.getFullGcCount(),
                taskStatus.getFullGcTimeInMillis(),
                taskStatus.getTotalCpuTimeInNanos(),
                taskStatus.getTaskAgeInMillis(),
                taskStatus.getQueuedPartitionedSplitsWeight(),
                taskStatus.getRunningPartitionedSplitsWeight());
    }

    public static TaskStatus toTaskStatus(ThriftTaskStatus thriftTaskStatus)
    {
        try {
            URI location = new URI(thriftTaskStatus.getSelf());

            return new TaskStatus(
                    thriftTaskStatus.getTaskInstanceIdLeastSignificantBits(),
                    thriftTaskStatus.getTaskInstanceIdMostSignificantBits(),
                    thriftTaskStatus.getVersion(),
                    toTaskState(thriftTaskStatus.getState()),
                    location,
                    thriftTaskStatus.getCompletedDriverGroups().stream().map(ThriftLifespanUtils::toLifespan).collect(Collectors.toSet()),
                    thriftTaskStatus.getFailures().stream().map(ThriftExecutionFailureInfoUtils::toExecutionFailureInfo).collect(Collectors.toList()),
                    thriftTaskStatus.getQueuedPartitionedDrivers(),
                    thriftTaskStatus.getRunningPartitionedDrivers(),
                    thriftTaskStatus.getOutputBufferUtilization(),
                    thriftTaskStatus.outputBufferOverutilized,
                    thriftTaskStatus.getPhysicalWrittenDataSizeInBytes(),
                    thriftTaskStatus.getMemoryReservationInBytes(),
                    thriftTaskStatus.getSystemMemoryReservationInBytes(),
                    thriftTaskStatus.getPeakNodeTotalMemoryReservationInBytes(),
                    thriftTaskStatus.getFullGcCount(),
                    thriftTaskStatus.getFullGcTimeInMillis(),
                    thriftTaskStatus.getTotalCpuTimeInNanos(),
                    thriftTaskStatus.getTaskAgeInMillis(),
                    thriftTaskStatus.getQueuedPartitionedSplitsWeight(),
                    thriftTaskStatus.getRunningPartitionedSplitsWeight());
        }
        catch (URISyntaxException e) {
            throw new PrestoException(SYNTAX_ERROR, format("Invalid URI string: %s", e.getMessage()));
        }
    }
}
