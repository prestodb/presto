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
    private final URI self;
    private final Optional<PlanFragment> plan;

    private final StageExecutionInfo latestAttemptExecutionInfo;
    private final List<StageExecutionInfo> previousAttemptsExecutionInfos;

    private final List<StageInfo> subStages;

    private final boolean isRuntimeOptimized;

    @JsonCreator
    public StageInfo(
            @JsonProperty("stageId") StageId stageId,
            @JsonProperty("self") URI self,
            @JsonProperty("plan") Optional<PlanFragment> plan,
            @JsonProperty("latestAttemptExecutionInfo") StageExecutionInfo latestAttemptExecutionInfo,
            @JsonProperty("previousAttemptsExecutionInfos") List<StageExecutionInfo> previousAttemptsExecutionInfos,
            @JsonProperty("subStages") List<StageInfo> subStages,
            @JsonProperty("isRuntimeOptimized") boolean isRuntimeOptimized)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.self = requireNonNull(self, "self is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.latestAttemptExecutionInfo = requireNonNull(latestAttemptExecutionInfo, "latestAttemptExecutionInfo is null");
        this.previousAttemptsExecutionInfos = ImmutableList.copyOf(requireNonNull(previousAttemptsExecutionInfos, "previousAttemptsExecutionInfos is null"));
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "subStages is null"));
        this.isRuntimeOptimized = isRuntimeOptimized;
    }

    @JsonProperty
    public StageId getStageId()
    {
        return stageId;
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
    public StageExecutionInfo getLatestAttemptExecutionInfo()
    {
        return latestAttemptExecutionInfo;
    }

    @JsonProperty
    public List<StageExecutionInfo> getPreviousAttemptsExecutionInfos()
    {
        return previousAttemptsExecutionInfos;
    }

    @JsonProperty
    public List<StageInfo> getSubStages()
    {
        return subStages;
    }

    @JsonProperty
    public boolean isRuntimeOptimized()
    {
        return isRuntimeOptimized;
    }

    public boolean isFinalStageInfo()
    {
        return latestAttemptExecutionInfo.isFinal();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", latestAttemptExecutionInfo.getState())
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

    public Optional<StageInfo> getStageWithStageId(StageId stageId)
    {
        Iterable<StageInfo> iterableStageInfo = forTree(StageInfo::getSubStages).depthFirstPreOrder(this);
        for (StageInfo stageInfo : iterableStageInfo) {
            if (stageInfo.getStageId().equals(stageId)) {
                return Optional.of(stageInfo);
            }
        }
        return Optional.empty();
    }
}
