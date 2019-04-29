/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

@Immutable
public class StageInfo
{
    private final StageId stageId;
    private final StageState state;
    private final URI self;
    private final Optional<PlanFragment> plan;
    private final List<Type> types;
    private final StageStats stageStats;
    private final List<TaskInfo> tasks;
    private final List<StageInfo> subStages;
    private final Optional<ExecutionFailureInfo> failureCause;

    @JsonCreator
    public StageInfo(
            @JsonProperty("stageId") StageId stageId,
            @JsonProperty("state") StageState state,
            @JsonProperty("self") URI self,
            @JsonProperty("plan") Optional<PlanFragment> plan,
            @JsonProperty("types") List<Type> types,
            @JsonProperty("stageStats") StageStats stageStats,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("subStages") List<StageInfo> subStages,
            @JsonProperty("failureCause") Optional<ExecutionFailureInfo> failureCause)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.state = requireNonNull(state, "state is null");
        this.self = requireNonNull(self, "self is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.stageStats = requireNonNull(stageStats, "stageStats is null");
        this.tasks = ImmutableList.copyOf(requireNonNull(tasks, "tasks is null"));
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "subStages is null"));
        this.failureCause = requireNonNull(failureCause, "failureCause is null");
    }

    @JsonProperty
    public StageId getStageId()
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
    public Optional<PlanFragment> getPlan()
    {
        return plan;
    }

    @JsonProperty
    public List<Type> getTypes()
    {
        return types;
    }

    @JsonProperty
    public StageStats getStageStats()
    {
        return stageStats;
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

    @JsonProperty
    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return failureCause;
    }

    public boolean isFinalStageInfo()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().getState().isDone());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }

    public List<StageInfo> getAllStages()
    {
        return ImmutableList.copyOf(forTree(StageInfo::getSubStages).depthFirstPreOrder(this));
    }

    public static List<StageInfo> getAllStages(Optional<StageInfo> stageInfo)
    {
        return stageInfo.map(StageInfo::getAllStages).orElse(ImmutableList.of());
    }
}
