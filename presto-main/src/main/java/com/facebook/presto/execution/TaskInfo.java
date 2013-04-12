/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.ExecutionStats.ExecutionStatsSnapshot;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final SharedBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final ExecutionStatsSnapshot stats;
    private final List<SplitExecutionStats> splitStats;
    private final List<FailureInfo> failures;
    private final Map<PlanNodeId, Set<?>> outputs;

    @JsonCreator
    public TaskInfo(@JsonProperty("taskId") TaskId taskId,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("outputBuffers") SharedBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") ExecutionStatsSnapshot stats,
            @JsonProperty("splitStats") List<SplitExecutionStats> splitStats,
            @JsonProperty("failures") List<FailureInfo> failures,
            @JsonProperty("outputs") Map<PlanNodeId, Set<?>> outputs)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(outputBuffers, "outputBufferStates is null");
        Preconditions.checkNotNull(noMoreSplits, "noMoreSplits is null");
        Preconditions.checkNotNull(stats, "stats is null");
        Preconditions.checkNotNull(failures, "failures is null");
        Preconditions.checkNotNull(outputs, "outputs is null");

        this.taskId = taskId;
        this.version = version;
        this.state = state;
        this.self = self;
        this.outputBuffers = outputBuffers;
        this.noMoreSplits = noMoreSplits;
        this.stats = stats;

        if (splitStats != null) {
            this.splitStats = ImmutableList.copyOf(splitStats);
        }
        else {
            this.splitStats = ImmutableList.of();
        }

        if (failures != null) {
            this.failures = ImmutableList.copyOf(failures);
        }
        else {
            this.failures = ImmutableList.of();
        }

        this.outputs = ImmutableMap.copyOf(outputs);
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
    public ExecutionStatsSnapshot getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<SplitExecutionStats> getSplitStats()
    {
        return splitStats;
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
