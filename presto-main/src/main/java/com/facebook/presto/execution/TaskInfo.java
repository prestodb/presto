/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

@Immutable
public class TaskInfo
{
    private final String queryId;
    private final String stageId;
    private final String taskId;
    private final TaskState state;
    private final URI self;
    private final List<PageBufferInfo> outputBuffers;
    private final ExecutionStats stats;
    private final List<FailureInfo> failures;

    @JsonCreator
    public TaskInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("stageId") String stageId,
            @JsonProperty("taskId") String taskId,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("outputBuffers") List<PageBufferInfo> outputBuffers,
            @JsonProperty("stats") ExecutionStats stats,
            @JsonProperty("failures") List<FailureInfo> failures)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(outputBuffers, "outputBufferStates is null");
        Preconditions.checkArgument(!outputBuffers.isEmpty(), "outputBufferStates is empty");
        Preconditions.checkNotNull(stats, "stats is null");
        Preconditions.checkNotNull(failures, "failures is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.state = state;
        this.self = self;
        this.outputBuffers = ImmutableList.copyOf(outputBuffers);
        this.stats = stats;
        this.failures = failures;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public String getTaskId()
    {
        return taskId;
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
    public List<PageBufferInfo> getOutputBuffers()
    {
        return outputBuffers;
    }

    @JsonProperty
    public ExecutionStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<FailureInfo> getFailures()
    {
        return failures;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("state", state)
                .toString();
    }


    public static Function<TaskInfo, String> taskIdGetter()
    {
        return new Function<TaskInfo, String>()
        {
            @Override
            public String apply(TaskInfo taskInfo)
            {
                return taskInfo.getTaskId();
            }
        };
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
