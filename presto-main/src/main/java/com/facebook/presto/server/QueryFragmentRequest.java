package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

public class QueryFragmentRequest
{
    private final String queryId;
    private final String stageId;
    private final PlanFragment fragment;
    private final List<PlanFragmentSource> splits;
    private final Map<String, ExchangePlanFragmentSource> exchangeSources;
    private final List<String> outputIds;

    @JsonCreator
    public QueryFragmentRequest(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("stageId") String stageId,
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("splits") List<PlanFragmentSource> splits,
            @JsonProperty("exchangeSources") Map<String, ExchangePlanFragmentSource> exchangeSources,
            @JsonProperty("outputIds") List<String> outputIds)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(splits, "splits is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.fragment = fragment;
        this.splits = ImmutableList.copyOf(splits);
        this.exchangeSources = ImmutableMap.copyOf(exchangeSources);
        this.outputIds = ImmutableList.copyOf(outputIds);
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
    public List<PlanFragmentSource> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public Map<String, ExchangePlanFragmentSource> getExchangeSources()
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
                .add("queryId", queryId)
                .add("stageId", stageId)
                .add("fragment", fragment)
                .add("splits", splits)
                .add("exchangeSources", exchangeSources)
                .add("outputIds", outputIds)
                .toString();
    }
}
