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

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class StreamingPlanSection
{
    private final StreamingSubPlan plan;
    // materialized exchange children
    private final List<StreamingPlanSection> children;

    public StreamingPlanSection(StreamingSubPlan plan, List<StreamingPlanSection> children)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public StreamingSubPlan getPlan()
    {
        return plan;
    }

    public List<StreamingPlanSection> getChildren()
    {
        return children;
    }

    public static StreamingPlanSection extractStreamingSections(SubPlan subPlan)
    {
        ImmutableList.Builder<SubPlan> materializedExchangeChildren = ImmutableList.builder();
        StreamingSubPlan streamingSection = extractStreamingSection(subPlan, materializedExchangeChildren);
        return new StreamingPlanSection(
                streamingSection,
                materializedExchangeChildren.build().stream()
                        .map(StreamingPlanSection::extractStreamingSections)
                        .collect(toImmutableList()));
    }

    private static StreamingSubPlan extractStreamingSection(SubPlan subPlan, ImmutableList.Builder<SubPlan> materializedExchangeChildren)
    {
        ImmutableList.Builder<StreamingSubPlan> streamingSources = ImmutableList.builder();
        Set<PlanFragmentId> streamingFragmentIds = subPlan.getFragment().getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        for (SubPlan child : subPlan.getChildren()) {
            if (streamingFragmentIds.contains(child.getFragment().getId())) {
                streamingSources.add(extractStreamingSection(child, materializedExchangeChildren));
            }
            else {
                materializedExchangeChildren.add(child);
            }
        }
        return new StreamingSubPlan(subPlan.getFragment(), streamingSources.build());
    }
}
