/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class QueryTaskInfo
{
    private final String taskId;
    private final List<TupleInfo> tupleInfos;
    private final QueryState.State state;
    private final int bufferedPages;
    private final int splits;
    private final int startedSplits;
    private final int completedSplits;
    private final long splitCpuTime;
    private final long inputDataSize;
    private final long inputPositionCount;
    private final long completedDataSize;
    private final long completedPositionCount;
    private final long outputDataSize;
    private final long outputPositionCount;

    public QueryTaskInfo(String taskId, List<TupleInfo> tupleInfos)
    {
        this(taskId, tupleInfos, State.PREPARING, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @JsonCreator
    public QueryTaskInfo(@JsonProperty("taskId") String taskId,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("state") State state,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("splits") int splits,
            @JsonProperty("startedSplits") int startedSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("splitCpuTime") long splitCpuTime,
            @JsonProperty("inputDataSize") long inputDataSize,
            @JsonProperty("inputPositionCount") long inputPositionCount,
            @JsonProperty("completedDataSize") long completedDataSize,
            @JsonProperty("completedPositionCount") long completedPositionCount,
            @JsonProperty("outputDataSize") long outputDataSize,
            @JsonProperty("outputPositionCount") long outputPositionCount)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        this.taskId = taskId;
        this.tupleInfos = tupleInfos;
        this.state = state;
        this.bufferedPages = bufferedPages;
        this.splits = splits;
        this.startedSplits = startedSplits;
        this.completedSplits = completedSplits;
        this.splitCpuTime = splitCpuTime;
        this.inputDataSize = inputDataSize;
        this.inputPositionCount = inputPositionCount;
        this.completedDataSize = completedDataSize;
        this.completedPositionCount = completedPositionCount;
        this.outputDataSize = outputDataSize;
        this.outputPositionCount = outputPositionCount;
    }

    @JsonProperty
    public String getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @JsonProperty
    public State getState()
    {
        return state;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public int getSplits()
    {
        return splits;
    }

    @JsonProperty
    public int getStartedSplits()
    {
        return startedSplits;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    public long getSplitCpuTime()
    {
        return splitCpuTime;
    }

    @JsonProperty
    public long getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    public long getInputPositionCount()
    {
        return inputPositionCount;
    }

    @JsonProperty
    public long getCompletedDataSize()
    {
        return completedDataSize;
    }

    @JsonProperty
    public long getCompletedPositionCount()
    {
        return completedPositionCount;
    }

    @JsonProperty
    public long getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositionCount()
    {
        return outputPositionCount;
    }
}
