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

import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.metadata.MetadataUpdates;
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

import static com.facebook.presto.execution.TaskStatus.initialTaskStatus;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.metadata.MetadataUpdates.DEFAULT_METADATA_UPDATES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class TaskInfo
{
    private final TaskId taskId;
    private final TaskStatus taskStatus;
    private final DateTime lastHeartbeat;
    private final OutputBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;

    private final boolean needsPlan;
    private final MetadataUpdates metadataUpdates;
    private final String nodeId;

    @JsonCreator
    public TaskInfo(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") OutputBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("needsPlan") boolean needsPlan,
            @JsonProperty("metadataUpdates") MetadataUpdates metadataUpdates,
            @JsonProperty("nodeId") String nodeId)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = requireNonNull(noMoreSplits, "noMoreSplits is null");
        this.stats = requireNonNull(stats, "stats is null");

        this.needsPlan = needsPlan;
        this.metadataUpdates = metadataUpdates;
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

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

    @JsonProperty
    public TaskStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public boolean isNeedsPlan()
    {
        return needsPlan;
    }

    @JsonProperty
    public MetadataUpdates getMetadataUpdates()
    {
        return metadataUpdates;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    public TaskInfo summarize()
    {
        if (taskStatus.getState().isDone()) {
            return new TaskInfo(
                    taskId,
                    taskStatus,
                    lastHeartbeat,
                    outputBuffers.summarize(),
                    noMoreSplits,
                    stats.summarizeFinal(),
                    needsPlan,
                    metadataUpdates,
                    nodeId);
        }
        return new TaskInfo(
                taskId,
                taskStatus,
                lastHeartbeat,
                outputBuffers.summarize(),
                noMoreSplits,
                stats.summarize(),
                needsPlan,
                metadataUpdates,
                nodeId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("state", taskStatus.getState())
                .toString();
    }

    public static TaskInfo createInitialTask(TaskId taskId, URI location, List<BufferInfo> bufferStates, TaskStats taskStats, String nodeId)
    {
        return new TaskInfo(
                taskId,
                initialTaskStatus(location),
                DateTime.now(),
                new OutputBufferInfo("UNINITIALIZED", OPEN, true, true, 0, 0, 0, 0, bufferStates),
                ImmutableSet.of(),
                taskStats,
                true,
                DEFAULT_METADATA_UPDATES,
                nodeId);
    }

    public TaskInfo withTaskStatus(TaskStatus newTaskStatus)
    {
        return new TaskInfo(taskId, newTaskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, needsPlan, metadataUpdates, nodeId);
    }
}
