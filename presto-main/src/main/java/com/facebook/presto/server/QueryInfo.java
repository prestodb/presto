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

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

@Immutable
public class QueryInfo
{
    private final String queryId;
    private final List<TupleInfo> tupleInfos;
    private final QueryState.State state;
    private final String outputStage;
    private final Map<String, List<QueryTaskInfo>> stages;

    public QueryInfo(String queryId, List<TupleInfo> tupleInfos)
    {
        this(queryId, tupleInfos, State.PREPARING, null, ImmutableMap.<String, List<QueryTaskInfo>>of());
    }

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("state") State state,
            @JsonProperty("outputStage") String outputStage,
            @JsonProperty("stages") Map<String, List<QueryTaskInfo>> stages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(stages, "stages is null");
        this.queryId = queryId;
        this.tupleInfos = tupleInfos;
        this.state = state;
        this.outputStage = outputStage;
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
    public String getOutputStage()
    {
        return outputStage;
    }

    @JsonProperty
    public Map<String, List<QueryTaskInfo>> getStages()
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
}
