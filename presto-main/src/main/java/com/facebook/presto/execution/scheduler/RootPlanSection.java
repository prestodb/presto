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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class RootPlanSection
{
    private final StreamingSubPlan plan;

    public RootPlanSection(StreamingSubPlan plan)
    {
        this.plan = requireNonNull(plan, "plan is null");
    }

    public StreamingSubPlan getPlan()
    {
        return plan;
    }

    public static RootPlanSection extractStreamingSections(SubPlan subPlan)
    {
        StreamingSubPlan streamingSection = extractStreamingSection(subPlan, new HashMap<>());
        return new RootPlanSection(streamingSection);
    }

    private static StreamingSubPlan extractStreamingSection(SubPlan subPlan, Map<PlanFragmentId, StreamingSubPlan> fragmentIdStreamingSubPlanMap)
    {
        ImmutableList.Builder<StreamingSubPlan> materializedExchangeSources = ImmutableList.builder();
        ImmutableList.Builder<StreamingSubPlan> streamingSources = ImmutableList.builder();
        Set<PlanFragmentId> streamingFragmentIds = subPlan.getFragment().getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        for (SubPlan child : subPlan.getChildren()) {
            if (streamingFragmentIds.contains(child.getFragment().getId())) {
                streamingSources.add(extractStreamingSection(child, fragmentIdStreamingSubPlanMap));
            }
            else {
                materializedExchangeSources.add(extractStreamingSection(child, fragmentIdStreamingSubPlanMap));
            }
        }
        if (!fragmentIdStreamingSubPlanMap.containsKey(subPlan.getFragment().getId())) {
            fragmentIdStreamingSubPlanMap.put(subPlan.getFragment().getId(),
                    new StreamingSubPlan(subPlan.getFragment(), streamingSources.build(), materializedExchangeSources.build()));
        }
        return fragmentIdStreamingSubPlanMap.get(subPlan.getFragment().getId());
    }
}
