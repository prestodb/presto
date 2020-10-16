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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.execution.TaskState.PLANNED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TaskStatus
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

    private final long taskInstanceIdLeastSignificantBits;
    private final long taskInstanceIdMostSignificantBits;
    private final long version;
    private final TaskState state;
    private final URI self;
    private final Set<Lifespan> completedDriverGroups;

    private final int queuedPartitionedDrivers;
    private final int runningPartitionedDrivers;

    private final double outputBufferUtilization;
    private final boolean outputBufferOverutilized;

    private final long physicalWrittenDataSizeInBytes;
    private final long memoryReservationInBytes;
    private final long systemMemoryReservationInBytes;
    private final long peakNodeTotalMemoryReservationInBytes;

    private final long fullGcCount;
    private final long fullGcTimeInMillis;

    private final List<ExecutionFailureInfo> failures;

    @JsonCreator
    @ThriftConstructor
    public TaskStatus(
            @JsonProperty("taskInstanceIdLeastSignificantBits") long taskInstanceIdLeastSignificantBits,
            @JsonProperty("taskInstanceIdMostSignificantBits") long taskInstanceIdMostSignificantBits,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("completedDriverGroups") Set<Lifespan> completedDriverGroups,
            @JsonProperty("failures") List<ExecutionFailureInfo> failures,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("outputBufferUtilization") double outputBufferUtilization,
            @JsonProperty("outputBufferOverutilized") boolean outputBufferOverutilized,
            @JsonProperty("physicalWrittenDataSizeInBytes") long physicalWrittenDataSizeInBytes,
            @JsonProperty("memoryReservationInBytes") long memoryReservationInBytes,
            @JsonProperty("systemMemoryReservationInBytes") long systemMemoryReservationInBytes,
            @JsonProperty("peakNodeTotalMemoryReservationInBytes") long peakNodeTotalMemoryReservationInBytes,
            @JsonProperty("fullGcCount") long fullGcCount,
            @JsonProperty("fullGcTimeInMillis") long fullGcTimeInMillis)
    {
        this.taskInstanceIdLeastSignificantBits = taskInstanceIdLeastSignificantBits;
        this.taskInstanceIdMostSignificantBits = taskInstanceIdMostSignificantBits;
        checkState(version >= MIN_VERSION, "version must be >= MIN_VERSION");
        this.version = version;
        this.state = requireNonNull(state, "state is null");
        this.self = requireNonNull(self, "self is null");
        this.completedDriverGroups = requireNonNull(completedDriverGroups, "completedDriverGroups is null");

        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers must be positive");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;

        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers must be positive");
        this.runningPartitionedDrivers = runningPartitionedDrivers;

        this.outputBufferUtilization = outputBufferUtilization;
        this.outputBufferOverutilized = outputBufferOverutilized;

        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;

        this.memoryReservationInBytes = memoryReservationInBytes;
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;
        this.peakNodeTotalMemoryReservationInBytes = peakNodeTotalMemoryReservationInBytes;
        this.failures = ImmutableList.copyOf(requireNonNull(failures, "failures is null"));

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTimeInMillis = fullGcTimeInMillis;
    }

    @JsonProperty
    @ThriftField(1)
    public long getTaskInstanceIdLeastSignificantBits()
    {
        return taskInstanceIdLeastSignificantBits;
    }

    @JsonProperty
    @ThriftField(2)
    public long getTaskInstanceIdMostSignificantBits()
    {
        return taskInstanceIdMostSignificantBits;
    }

    @JsonProperty
    @ThriftField(3)
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    @ThriftField(4)
    public TaskState getState()
    {
        return state;
    }

    @JsonProperty
    @ThriftField(5)
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    @ThriftField(6)
    public Set<Lifespan> getCompletedDriverGroups()
    {
        return completedDriverGroups;
    }

    @JsonProperty
    @ThriftField(7)
    public List<ExecutionFailureInfo> getFailures()
    {
        return failures;
    }

    @JsonProperty
    @ThriftField(8)
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    @ThriftField(9)
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    @ThriftField(10)
    public double getOutputBufferUtilization()
    {
        return outputBufferUtilization;
    }

    @JsonProperty
    @ThriftField(11)
    public boolean isOutputBufferOverutilized()
    {
        return outputBufferOverutilized;
    }

    @JsonProperty
    @ThriftField(12)
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    @JsonProperty
    @ThriftField(13)
    public long getMemoryReservationInBytes()
    {
        return memoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(14)
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

    @JsonProperty
    @ThriftField(15)
    public long getFullGcCount()
    {
        return fullGcCount;
    }

    @JsonProperty
    @ThriftField(16)
    public long getFullGcTimeInMillis()
    {
        return fullGcTimeInMillis;
    }

    @JsonProperty
    @ThriftField(17)
    public long getPeakNodeTotalMemoryReservationInBytes()
    {
        return peakNodeTotalMemoryReservationInBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .toString();
    }

    public static TaskStatus initialTaskStatus(URI location)
    {
        return new TaskStatus(
                0L,
                0L,
                MIN_VERSION,
                PLANNED,
                location,
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
                0);
    }

    public static TaskStatus failWith(TaskStatus taskStatus, TaskState state, List<ExecutionFailureInfo> exceptions)
    {
        return new TaskStatus(
                taskStatus.getTaskInstanceIdLeastSignificantBits(),
                taskStatus.getTaskInstanceIdMostSignificantBits(),
                MAX_VERSION,
                state,
                taskStatus.getSelf(),
                taskStatus.getCompletedDriverGroups(),
                exceptions,
                taskStatus.getQueuedPartitionedDrivers(),
                taskStatus.getRunningPartitionedDrivers(),
                taskStatus.getOutputBufferUtilization(),
                taskStatus.isOutputBufferOverutilized(),
                taskStatus.getPhysicalWrittenDataSizeInBytes(),
                taskStatus.getMemoryReservationInBytes(),
                taskStatus.getSystemMemoryReservationInBytes(),
                taskStatus.getPeakNodeTotalMemoryReservationInBytes(),
                taskStatus.getFullGcCount(),
                taskStatus.getFullGcTimeInMillis());
    }
}
