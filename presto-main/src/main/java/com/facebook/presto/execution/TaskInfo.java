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

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final long version;
    private final TaskState state;
    private final URI self;
    private final DateTime lastHeartbeat;
    private final SharedBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;
    private final List<FailureInfo> failures;
    private final Map<PlanNodeId, Set<?>> outputs;

    @JsonCreator
    public TaskInfo(@JsonProperty("taskId") TaskId taskId,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") SharedBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("failures") List<FailureInfo> failures,
            @JsonProperty("outputs") Map<PlanNodeId, Set<?>> outputs)
    {
        this.taskId = checkNotNull(taskId, "taskId is null");
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

        this.outputs = ImmutableMap.copyOf(checkNotNull(outputs, "outputs is null"));
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
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
    public List<FailureInfo> getFailures()
    {
        return failures;
    }

    @JsonProperty
    public Map<PlanNodeId, Set<?>> getOutputs()
    {
        return outputs;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("state", state)
                .toString();
    }

    public static Function<TaskInfo, TaskState> taskStateGetter()
    {
        return new Function<TaskInfo, TaskState>()
        {
            @Override
            public TaskState apply(TaskInfo taskInfo)
            {
                return taskInfo.getState();
            }
        };
    }
}
