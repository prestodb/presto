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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.execution.TaskState.PLANNED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private static final JsonCodec<List<ExecutionFailureInfo>> LIST_EXECUTION_FAILURE_INFO_CODEC = listJsonCodec(ExecutionFailureInfo.class);

    private final TaskId taskId;
    private final String taskInstanceId;
    private final long version;
    private final TaskState state;
    private final URI self;
    private final String nodeId;
    private final Set<Lifespan> completedDriverGroups;

    private final int queuedPartitionedDrivers;
    private final int runningPartitionedDrivers;

    private final double outputBufferUtilization;
    private final boolean outputBufferOverutilized;

    private final DataSize physicalWrittenDataSize;
    private final DataSize memoryReservation;
    private final DataSize systemMemoryReservation;

    private final long fullGcCount;
    private final Duration fullGcTime;

    private final List<ExecutionFailureInfo> failures;

    @ThriftConstructor
    public TaskStatus(
            TaskId taskId,
            String taskInstanceId,
            long version,
            TaskState state,
            String self,
            String nodeId,
            Set<Lifespan> completedDriverGroups,
            String failures,
            int queuedPartitionedDrivers,
            int runningPartitionedDrivers,
            double outputBufferUtilization,
            boolean outputBufferOverutilized,
            String physicalWrittenDataSize,
            String memoryReservation,
            String systemMemoryReservation,
            long fullGcCount,
            String fullGcTime)
    {
        this(
                taskId,
                taskInstanceId,
                version,
                state,
                URI.create(self),
                nodeId,
                completedDriverGroups,
                LIST_EXECUTION_FAILURE_INFO_CODEC.fromJson(failures),
                queuedPartitionedDrivers,
                runningPartitionedDrivers,
                outputBufferUtilization,
                outputBufferOverutilized,
                DataSize.valueOf(physicalWrittenDataSize),
                DataSize.valueOf(memoryReservation),
                DataSize.valueOf(systemMemoryReservation),
                fullGcCount,
                Duration.valueOf(fullGcTime));
    }

    @JsonCreator
    public TaskStatus(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("taskInstanceId") String taskInstanceId,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("completedDriverGroups") Set<Lifespan> completedDriverGroups,
            @JsonProperty("failures") List<ExecutionFailureInfo> failures,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("outputBufferUtilization") double outputBufferUtilization,
            @JsonProperty("outputBufferOverutilized") boolean outputBufferOverutilized,
            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,
            @JsonProperty("memoryReservation") DataSize memoryReservation,
            @JsonProperty("systemMemoryReservation") DataSize systemMemoryReservation,
            @JsonProperty("fullGcCount") long fullGcCount,
            @JsonProperty("fullGcTime") Duration fullGcTime)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");

        checkState(version >= MIN_VERSION, "version must be >= MIN_VERSION");
        this.version = version;
        this.state = requireNonNull(state, "state is null");
        this.self = requireNonNull(self, "self is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.completedDriverGroups = requireNonNull(completedDriverGroups, "completedDriverGroups is null");

        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers must be positive");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;

        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers must be positive");
        this.runningPartitionedDrivers = runningPartitionedDrivers;

        this.outputBufferUtilization = outputBufferUtilization;
        this.outputBufferOverutilized = outputBufferOverutilized;

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");

        this.memoryReservation = requireNonNull(memoryReservation, "memoryReservation is null");
        this.systemMemoryReservation = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null");
        this.failures = ImmutableList.copyOf(requireNonNull(failures, "failures is null"));

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTime = requireNonNull(fullGcTime, "fullGcTime is null");
    }

    @ThriftField(1)
    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @ThriftField(2)
    @JsonProperty
    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    @ThriftField(3)
    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @ThriftField(4)
    @JsonProperty
    public TaskState getState()
    {
        return state;
    }

    @ThriftField(value = 5, name = "self")
    public String getSelfString()
    {
        return self.toString();
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @ThriftField(6)
    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @ThriftField(7)
    @JsonProperty
    public Set<Lifespan> getCompletedDriverGroups()
    {
        return completedDriverGroups;
    }

    @ThriftField(value = 8, name = "failures")
    public String getFailuresJson()
    {
        return LIST_EXECUTION_FAILURE_INFO_CODEC.toJson(failures);
    }

    @JsonProperty
    public List<ExecutionFailureInfo> getFailures()
    {
        return failures;
    }

    @ThriftField(9)
    @JsonProperty
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @ThriftField(10)
    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @ThriftField(11)
    @JsonProperty
    public double getOutputBufferUtilization()
    {
        return outputBufferUtilization;
    }

    @ThriftField(12)
    @JsonProperty
    public boolean isOutputBufferOverutilized()
    {
        return outputBufferOverutilized;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @ThriftField(value = 13, name = "physicalWrittenDataSize")
    public String getPhysicalWrittenDataSizeString()
    {
        return physicalWrittenDataSize.toString();
    }

    @JsonProperty
    public DataSize getMemoryReservation()
    {
        return memoryReservation;
    }

    @ThriftField(value = 14, name = "memoryReservation")
    public String getMemoryReservationString()
    {
        return memoryReservation.toString();
    }

    @JsonProperty
    public DataSize getSystemMemoryReservation()
    {
        return systemMemoryReservation;
    }

    @ThriftField(value = 15, name = "systemMemoryReservation")
    public String getSystemMemoryReservationString()
    {
        return systemMemoryReservation.toString();
    }

    @ThriftField(16)
    @JsonProperty
    public long getFullGcCount()
    {
        return fullGcCount;
    }

    @JsonProperty
    public Duration getFullGcTime()
    {
        return fullGcTime;
    }

    @ThriftField(value = 17, name = "fullGcTime")
    public String getFullGcTimeString()
    {
        return fullGcTime.toString();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("state", state)
                .toString();
    }

    public static TaskStatus initialTaskStatus(TaskId taskId, URI location, String nodeId)
    {
        return new TaskStatus(
                taskId,
                "",
                MIN_VERSION,
                PLANNED,
                location,
                nodeId,
                ImmutableSet.of(),
                ImmutableList.of(),
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

    public static TaskStatus failWith(TaskStatus taskStatus, TaskState state, List<ExecutionFailureInfo> exceptions)
    {
        return new TaskStatus(
                taskStatus.getTaskId(),
                taskStatus.getTaskInstanceId(),
                MAX_VERSION,
                state,
                taskStatus.getSelf(),
                taskStatus.getNodeId(),
                taskStatus.getCompletedDriverGroups(),
                exceptions,
                taskStatus.getQueuedPartitionedDrivers(),
                taskStatus.getRunningPartitionedDrivers(),
                taskStatus.getOutputBufferUtilization(),
                taskStatus.isOutputBufferOverutilized(),
                taskStatus.getPhysicalWrittenDataSize(),
                taskStatus.getMemoryReservation(),
                taskStatus.getSystemMemoryReservation(),
                taskStatus.getFullGcCount(),
                taskStatus.getFullGcTime());
    }
}
