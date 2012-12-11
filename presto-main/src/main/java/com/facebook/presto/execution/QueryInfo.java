/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

@Immutable
public class QueryInfo
{
    private final String queryId;
    private final QueryState state;
    private final URI self;
    private final List<String> fieldNames;
    private final List<TupleInfo> tupleInfos;
    private final String query;
    private final QueryStats queryStats;
    private final StageInfo outputStage;

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("state") QueryState state,
            @JsonProperty("self") URI self,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("query") String query,
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("outputStage") StageInfo outputStage)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(fieldNames, "fieldNames is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(queryStats, "queryStats is null");
        Preconditions.checkNotNull(query, "query is null");

        this.queryId = queryId;
        this.state = state;
        this.self = self;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.query = query;
        this.queryStats = queryStats;
        this.outputStage = outputStage;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @JsonProperty
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public StageInfo getOutputStage()
    {
        return outputStage;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .add("fieldNames", fieldNames)
                .toString();
    }
}
