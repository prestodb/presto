/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class MasterQueryState
{
    private final String queryId;
    private final QueryState outputQueryState;
    private final ConcurrentHashMap<String, List<HttpQueryProvider>> stages = new ConcurrentHashMap<>();

    public MasterQueryState(String queryId, QueryState outputQueryState)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(outputQueryState, "outputQueryState is null");

        this.queryId = queryId;
        this.outputQueryState = outputQueryState;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public QueryState getOutputQueryState()
    {
        return outputQueryState;
    }

    public List<HttpQueryProvider> getStage(String stageId)
    {
        return stages.get(stageId);
    }

    public void addStage(String stageId, Iterable<HttpQueryProvider> value)
    {
        stages.put(stageId, ImmutableList.copyOf(value));
    }

    public QueryInfo toQueryInfo()
    {
        ImmutableMap.Builder<String, List<QueryInfo>> map = ImmutableMap.builder();
        for (Entry<String, List<HttpQueryProvider>> stage : stages.entrySet()) {
            map.put(stage.getKey(), ImmutableList.copyOf(Iterables.transform(stage.getValue(), new Function<HttpQueryProvider, QueryInfo>()
            {
                @Override
                public QueryInfo apply(HttpQueryProvider queryProvider)
                {
                    QueryInfo queryInfo = queryProvider.getQueryInfo();
                    return new QueryInfo(queryProvider.getLocation().toString(),
                            queryProvider.getTupleInfos(),
                            queryInfo.getState(),
                            queryInfo.getBufferedPages(),
                            queryInfo.getSplits(),
                            queryInfo.getStartedSplits(),
                            queryInfo.getCompletedSplits(),
                            queryInfo.getSplitCpuTime(),
                            queryInfo.getInputDataSize(),
                            queryInfo.getInputPositionCount(),
                            queryInfo.getCompletedDataSize(),
                            queryInfo.getCompletedPositionCount(),
                            ImmutableMap.<String, List<QueryInfo>>of());
                }
            })));
        }
        return outputQueryState.toQueryInfo(queryId, map.build());
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
        final MasterQueryState other = (MasterQueryState) obj;
        return Objects.equal(this.queryId, other.queryId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("outputQueryState", outputQueryState)
                .add("stages", stages)
                .toString();
    }
}
