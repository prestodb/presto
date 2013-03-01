package com.facebook.presto.server;

import com.facebook.presto.split.Split;
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
import java.util.Set;

public class QueryFragmentRequest
{
    private final Session session;
    private final String queryId;
    private final String stageId;
    private final PlanFragment fragment;
    private final ImmutableMap<PlanNodeId, Set<Split>> initialSources;
    private final List<String> initialOutputIds;

    @JsonCreator
    public QueryFragmentRequest(
            @JsonProperty("session") Session session,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("stageId") String stageId,
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("initialSources") Map<PlanNodeId, Set<Split>> initialSources,
            @JsonProperty("initialOutputIds") List<String> initialOutputIds)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(initialSources, "initialSources is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");

        this.session = session;
        this.queryId = queryId;
        this.stageId = stageId;
        this.fragment = fragment;
        this.initialSources = ImmutableMap.copyOf(initialSources);
        this.initialOutputIds = ImmutableList.copyOf(initialOutputIds);
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
    public Map<PlanNodeId, Set<Split>> getInitialSources()
    {
        return initialSources;
    }

    @JsonProperty
    public List<String> getInitialOutputIds()
    {
        return initialOutputIds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("session", session)
                .add("queryId", queryId)
                .add("stageId", stageId)
                .add("fragment", fragment)
                .add("initialSources", initialSources)
                .add("initialOutputIds", initialOutputIds)
                .toString();
    }
}
