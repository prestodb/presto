package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TaskUpdateRequest
{
    private final Session session;
    private final PlanFragment fragment;
    private final List<TaskSource> sources;
    private final OutputBuffers outputIds;

    @JsonCreator
    public TaskUpdateRequest(
            @JsonProperty("session") Session session,
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("sources") List<TaskSource> sources,
            @JsonProperty("outputIds") OutputBuffers outputIds)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");

        this.session = session;
        this.fragment = fragment;
        this.sources = ImmutableList.copyOf(sources);
        this.outputIds = outputIds;
    }

    @JsonProperty
    public Session getSession()
    {
        return session;
    }

    @JsonProperty
    public PlanFragment getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public List<TaskSource> getSources()
    {
        return sources;
    }

    @JsonProperty
    public OutputBuffers getOutputIds()
    {
        return outputIds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("session", session)
                .add("fragment", fragment)
                .add("sources", sources)
                .add("outputIds", outputIds)
                .toString();
    }
}
