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
    private final Map<String, List<QueryInfo>> stages;

    public QueryInfo(String queryId, List<TupleInfo> tupleInfos, QueryState.State state, int bufferedPages)
    {
        this(queryId, tupleInfos, state, bufferedPages, ImmutableMap.<String, List<QueryInfo>>of());
    }

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("state") QueryState.State state,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("stages") Map<String, List<QueryInfo>> stages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(stages, "stages is null");
        this.queryId = queryId;
        this.tupleInfos = tupleInfos;
        this.state = state;
        this.bufferedPages = bufferedPages;
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
                .add("stages", stages)
                .toString();
    }
}
