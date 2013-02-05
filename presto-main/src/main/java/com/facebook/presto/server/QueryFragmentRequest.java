package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class QueryFragmentRequest
{
    private final Session session;
    private final String queryId;
    private final String stageId;
    private final PlanFragment fragment;
    private final Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources;
    private final List<String> outputIds;

    @JsonCreator
    public QueryFragmentRequest(
            @JsonProperty("session") Session session,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("stageId") String stageId,
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("exchangeSources") Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            @JsonProperty("outputIds") List<String> outputIds)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");

        this.session = session;
        this.queryId = queryId;
        this.stageId = stageId;
        this.fragment = fragment;
        this.exchangeSources = ImmutableMap.copyOf(exchangeSources);
        this.outputIds = ImmutableList.copyOf(outputIds);
    }

    @JsonProperty
    public Session getSession()
    {
        return session;
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
    public PlanFragment getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public Map<PlanNodeId, ExchangePlanFragmentSource> getExchangeSources()
    {
        return exchangeSources;
    }

    @JsonProperty
    public List<String> getOutputIds()
    {
        return outputIds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("session", session)
                .add("queryId", queryId)
                .add("stageId", stageId)
                .add("fragment", fragment)
                .add("exchangeSources", exchangeSources)
                .add("outputIds", outputIds)
                .toString();
    }
}
