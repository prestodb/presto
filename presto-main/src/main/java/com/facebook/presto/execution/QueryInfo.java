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
import java.util.List;

@Immutable
public class QueryInfo
{
    private final String queryId;
    private final QueryState state;
    private final List<String> fieldNames;
    private final List<TupleInfo> tupleInfos;
    private final StageInfo outputStage;

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("state") QueryState state,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos,
            @JsonProperty("outputStage") StageInfo outputStage)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(fieldNames, "fieldNames is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        this.queryId = queryId;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.state = state;
        this.outputStage = outputStage;
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
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
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
