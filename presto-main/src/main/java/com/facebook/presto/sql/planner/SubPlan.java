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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Predicates.instanceOf;

@Immutable
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> children;

    public SubPlan(PlanFragment fragment, List<SubPlan> children)
    {
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(children, "children is null");

        this.fragment = fragment;
        this.children = ImmutableList.copyOf(children);
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    /**
     * Flattens the subplan and returns all PlanFragments in the tree
     */
    public List<PlanFragment> getAllFragments()
    {
        ImmutableList.Builder<PlanFragment> fragments = ImmutableList.builder();

        fragments.add(getFragment());
        for (SubPlan child : getChildren()) {
            fragments.addAll(child.getAllFragments());
        }

        return fragments.build();
    }

    public void sanityCheck()
    {
        Multiset<PlanFragmentId> exchangeIds = IterableTransformer.on(fragment.getSources())
                .select(instanceOf(ExchangeNode.class))
                .cast(ExchangeNode.class)
                .transformAndFlatten(ExchangeNode.sourceFragmentIdsGetter())
                .bag();

        Multiset<PlanFragmentId> childrenIds = IterableTransformer.on(children)
                .transform(SubPlan.fragmentGetter())
                .transform(PlanFragment.idGetter())
                .bag();

        Preconditions.checkState(exchangeIds.equals(childrenIds), "Subplan exchange ids don't match child fragment ids (%s vs %s)", exchangeIds, childrenIds);

        for (SubPlan child : children) {
            child.sanityCheck();
        }
    }

    public static Function<SubPlan, PlanFragment> fragmentGetter()
    {
        return new Function<SubPlan, PlanFragment>()
        {
            @Override
            public PlanFragment apply(SubPlan input)
            {
                return input.getFragment();
            }
        };
    }
}
