/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

public class QueryInfo
{
    private final String queryId;
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
    private final Map<String, List<QueryInfo>> stages;

    public QueryInfo(String queryId, List<TupleInfo> tupleInfos)
    {
        this(queryId, tupleInfos, State.PREPARING, 0, 0, 0, 0, 0, 0, 0, 0, 0, ImmutableMap.<String, List<QueryInfo>>of());
    }

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
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
            @JsonProperty("stages") Map<String, List<QueryInfo>> stages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(stages, "stages is null");
        this.queryId = queryId;
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
        this.stages = ImmutableMap.copyOf(stages);
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
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
    public Map<String, List<QueryInfo>> getStages()
    {
        return stages;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(queryId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final QueryInfo other = (QueryInfo) obj;
        return Objects.equal(this.queryId, other.queryId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .add("bufferedPages", bufferedPages)
                .add("splits", splits)
                .add("startedSplits", startedSplits)
                .add("completedSplits", completedSplits)
                .add("splitCpuTime", splitCpuTime)
                .add("inputDataSize", inputDataSize)
                .add("inputPositionCount", inputPositionCount)
                .add("completedDataSize", completedDataSize)
                .add("completedPositionCount", completedPositionCount)
                .add("tupleInfos", tupleInfos)
                .add("stages", stages)
                .toString();
    }
}
