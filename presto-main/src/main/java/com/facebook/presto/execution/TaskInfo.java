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
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.execution.TaskStatus.initialTaskStatus;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
@ThriftStruct
public class TaskInfo
{
    private final TaskStatus taskStatus;
    private final DateTime lastHeartbeat;
    private final OutputBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;

    private final boolean needsPlan;

    private static final JsonCodec<TaskStats> TASK_STATS_CODEC = jsonCodec(TaskStats.class);

    @ThriftConstructor
    public TaskInfo(TaskStatus taskStatus, String lastHeartbeat, OutputBufferInfo outputBuffers, Set<String> noMoreSplits, String stats, boolean needsPlan)
    {
        this(taskStatus, DateTime.parse(lastHeartbeat), outputBuffers, noMoreSplits.stream().map(PlanNodeId::new).collect(toImmutableSet()), TASK_STATS_CODEC.fromJson(stats), needsPlan);
    }

    @JsonCreator
    public TaskInfo(@JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") OutputBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("needsPlan") boolean needsPlan)
    {
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = requireNonNull(noMoreSplits, "noMoreSplits is null");
        this.stats = requireNonNull(stats, "stats is null");

        this.needsPlan = needsPlan;
    }

    @ThriftField(1)
    @JsonProperty
    public TaskStatus getTaskStatus()
    {
        return taskStatus;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @ThriftField(value = 2, name = "lastHeartbeat")
    public String getLastHeartbeatString()
    {
        return lastHeartbeat.toString();
    }

    @ThriftField(3)
    @JsonProperty
    public OutputBufferInfo getOutputBuffers()
    {
        return outputBuffers;
    }

    @JsonProperty
    public Set<PlanNodeId> getNoMoreSplits()
    {
        return noMoreSplits;
    }

    @ThriftField(value = 4, name = "noMoreSplits")
    public Set<String> getNoMoreSplitsStringSet()
    {
        return noMoreSplits.stream().map(PlanNodeId::toString).collect(toImmutableSet());
    }

    @JsonProperty
    public TaskStats getStats()
    {
        return stats;
    }

    @ThriftField(value = 5, name = "stats")
    public String getStatsJson()
    {
        return TASK_STATS_CODEC.toJson(stats);
    }

    @ThriftField(6)
    @JsonProperty
    public boolean isNeedsPlan()
    {
        return needsPlan;
    }

    public TaskInfo summarize()
    {
        if (taskStatus.getState().isDone()) {
            return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarize(), noMoreSplits, stats.summarizeFinal(), needsPlan);
        }
        return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarize(), noMoreSplits, stats.summarize(), needsPlan);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskStatus.getTaskId())
                .add("state", taskStatus.getState())
                .toString();
    }

    public static TaskInfo createInitialTask(TaskId taskId, URI location, String nodeId, List<BufferInfo> bufferStates, TaskStats taskStats)
    {
        return new TaskInfo(
                initialTaskStatus(taskId, location, nodeId),
                DateTime.now(),
                new OutputBufferInfo("UNINITIALIZED", OPEN, true, true, 0, 0, 0, 0, bufferStates),
                ImmutableSet.of(),
                taskStats,
                true);
    }

    public TaskInfo withTaskStatus(TaskStatus newTaskStatus)
    {
        return new TaskInfo(newTaskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, needsPlan);
    }
}
