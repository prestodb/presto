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

package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class FragmentedPlanSourceProvider
        implements PlanNodeSourceProvider
{
    private final Map<PlanFragmentId, PlanFragment> fragments;

    public static FragmentedPlanSourceProvider create(List<PlanFragment> planFragments)
    {
        Map<PlanFragmentId, PlanFragment> fragmentIdPlanNodeMap = planFragments.stream()
                .collect(toImmutableMap(PlanFragment::getId, fragment -> fragment));
        return new FragmentedPlanSourceProvider(fragmentIdPlanNodeMap);
    }

    private FragmentedPlanSourceProvider(Map<PlanFragmentId, PlanFragment> fragments)
    {
        this.fragments = ImmutableMap.copyOf(requireNonNull(fragments, "fragments is null"));
    }

    @Override
    public List<PlanNode> getSources(PlanNode node)
    {
        if (node instanceof RemoteSourceNode) {
            return ((RemoteSourceNode) node).getSourceFragmentIds().stream()
                    .map(id -> {
                        verify(fragments.containsKey(id), "fragment id not in map: %s", id);
                        return fragments.get(id).getRoot();
                    })
                    .collect(toImmutableList());
        }

        return node.getSources();
    }

    public List<PlanFragment> getSourceFragments(RemoteSourceNode node)
    {
        return node.getSourceFragmentIds().stream()
                .map(id -> {
                    verify(fragments.containsKey(id), "fragment id not in map: %s", id);
                    return fragments.get(id);
                })
                .collect(toImmutableList());
    }
}
