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

import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class TaskInfo
{
    /**
     * The first valid version that will be returned for a remote task.
     */
    public static final long STARTING_VERSION = 1;

    /**
     * A value lower than {@link #STARTING_VERSION}. This value can be used to
     * create an initial local task that is always older than any remote task.
     */
    public static final long MIN_VERSION = 0;

    /**
     * A value larger than any valid value. This value can be used to create
     * a final local task that is always newer than any remote task.
     */
    public static final long MAX_VERSION = Long.MAX_VALUE;

    private final TaskId taskId;
    private final Optional<String> nodeInstanceId;
    private final long version;
    private final TaskState state;
    private final URI self;
    private final DateTime lastHeartbeat;
    private final SharedBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;
    private final List<ExecutionFailureInfo> failures;

    @JsonCreator
    public TaskInfo(@JsonProperty("taskId") TaskId taskId,
            @JsonProperty("nodeInstanceId") Optional<String> nodeInstanceId,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") SharedBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("failures") List<ExecutionFailureInfo> failures)
    {
        this.taskId = checkNotNull(taskId, "taskId is null");
        this.nodeInstanceId = checkNotNull(nodeInstanceId, "nodeInstanceId is null");
        this.version = checkNotNull(version, "version is null");
        this.state = checkNotNull(state, "state is null");
        this.self = checkNotNull(self, "self is null");
        this.lastHeartbeat = checkNotNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = checkNotNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = checkNotNull(noMoreSplits, "noMoreSplits is null");
        this.stats = checkNotNull(stats, "stats is null");

        if (failures != null) {
            this.failures = ImmutableList.copyOf(failures);
        }
        else {
            this.failures = ImmutableList.of();
        }
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public Optional<String> getNodeInstanceId()
    {
        return nodeInstanceId;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public TaskState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @JsonProperty
    public SharedBufferInfo getOutputBuffers()
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
    public List<ExecutionFailureInfo> getFailures()
    {
        return failures;
    }

    public TaskInfo summarize()
    {
        return new TaskInfo(taskId, nodeInstanceId, version, state, self, lastHeartbeat, outputBuffers, noMoreSplits, stats.summarize(), failures);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("state", state)
                .toString();
    }
}
