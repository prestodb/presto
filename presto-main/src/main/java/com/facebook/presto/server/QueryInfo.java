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
    private final int completedSplits;
    private final Map<String, List<QueryInfo>> stages;

    public QueryInfo(String queryId, List<TupleInfo> tupleInfos, State state, int bufferedPages, int splits, int completedSplits)
    {
        this(queryId, tupleInfos, state, bufferedPages, splits, completedSplits, ImmutableMap.<String, List<QueryInfo>>of());
    }

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("state") State state,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("splits") int splits,
            @JsonProperty("completedSplits") int completedSplits,
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
        this.completedSplits = completedSplits;
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
    public int getCompletedSplits()
    {
        return completedSplits;
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
                .add("tupleInfos", tupleInfos)
                .add("state", state)
                .add("bufferedPages", bufferedPages)
                .add("splits", splits)
                .add("completedSplits", completedSplits)
                .add("stages", stages)
                .toString();
    }
}
