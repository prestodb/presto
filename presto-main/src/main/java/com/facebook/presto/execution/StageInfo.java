/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

@Immutable
public class StageInfo
{
    private final String queryId;
    private final String stageId;
    private final StageState state;
    private final URI self;
    private final PlanFragment plan;
    private final List<TaskInfo> tasks;
    private final List<StageInfo> subStages;

    @JsonCreator
    public StageInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("stageId") String stageId,
            @JsonProperty("state") StageState state,
            @JsonProperty("self") URI self,
            @JsonProperty("plan") @Nullable PlanFragment plan,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("subStages") List<StageInfo> subStages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(tasks, "tasks is null");
        Preconditions.checkNotNull(subStages, "subStages is null");
        this.queryId = queryId;
        this.stageId = stageId;
        this.state = state;
        this.self = self;
        this.plan = plan;
        this.tasks = ImmutableList.copyOf(tasks);
        this.subStages = subStages;
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
    public StageState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    @Nullable
    public PlanFragment getPlan()
    {
        return plan;
    }

    @JsonProperty
    public List<TaskInfo> getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public List<StageInfo> getSubStages()
    {
        return subStages;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }


    public static Function<StageInfo, StageState> stageStateGetter()
    {
        return new Function<StageInfo, StageState>()
        {
            @Override
            public StageState apply(StageInfo stageInfo)
            {
                return stageInfo.getState();
            }
        };
    }
}
