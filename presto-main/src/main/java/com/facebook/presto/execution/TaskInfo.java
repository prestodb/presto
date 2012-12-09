/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.PageBuffer.BufferState;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;
import java.util.Map;

@Immutable
public class TaskInfo
{
    private final String queryId;
    private final String stageId;
    private final String taskId;
    private final TaskState state;
    private final URI self;
    private final Map<String, BufferState> outputBufferStates;
    private final List<TupleInfo> tupleInfos;
    private final ExecutionStats stats;

    @JsonCreator
    public TaskInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("stageId")String stageId,
            @JsonProperty("taskId") String taskId,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("outputBufferStates") Map<String, BufferState> outputBufferStates,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("stats") ExecutionStats stats)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(outputBufferStates, "outputBufferStates is null");
        Preconditions.checkArgument(!outputBufferStates.isEmpty(), "outputBufferStates is empty");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(stats, "stats is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.state = state;
        this.self = self;
        this.outputBufferStates = ImmutableMap.copyOf(outputBufferStates);
        this.tupleInfos = tupleInfos;
        this.stats = stats;
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
    public Map<String, BufferState> getOutputBufferStates()
    {
        return outputBufferStates;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @JsonProperty
    public ExecutionStats getStats()
    {
        return stats;
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
