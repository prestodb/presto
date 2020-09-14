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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.CanonicalPlanFragment;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static java.util.Objects.requireNonNull;

public class FragmentResultCacheContext
{
    private static final Set<Class<? extends PlanNode>> ALLOWED_CHILDREN_NODES = ImmutableSet.of(TableScanNode.class, FilterNode.class, ProjectNode.class, GroupIdNode.class);

    private final FragmentResultCacheManager fragmentResultCacheManager;
    private final CanonicalPlanFragment canonicalPlanFragment;

    public static Optional<FragmentResultCacheContext> createFragmentResultCacheContext(
            FragmentResultCacheManager fragmentResultCacheManager,
            PlanNode root,
            PartitioningScheme partitioningScheme,
            Session session)
    {
        if (!SystemSessionProperties.isFragmentResultCachingEnabled(session) || !isEligibleForFragmentResultCaching(root)) {
            return Optional.empty();
        }

        Optional<CanonicalPlanFragment> canonicalPlanFragment = generateCanonicalPlan(root, partitioningScheme);
        return canonicalPlanFragment.map(fragment -> new FragmentResultCacheContext(fragmentResultCacheManager, fragment));
    }

    private static boolean isEligibleForFragmentResultCaching(PlanNode root)
    {
        if (!(root instanceof AggregationNode) || ((AggregationNode) root).getStep() != PARTIAL) {
            return false;
        }
        return root.getSources().stream().allMatch(FragmentResultCacheContext::containsOnlyAllowedNodesInChildren);
    }

    private static boolean containsOnlyAllowedNodesInChildren(PlanNode node)
    {
        if (!ALLOWED_CHILDREN_NODES.contains(node.getClass())) {
            return false;
        }
        return node.getSources().stream().allMatch(FragmentResultCacheContext::containsOnlyAllowedNodesInChildren);
    }

    private FragmentResultCacheContext(FragmentResultCacheManager fragmentResultCacheManager, CanonicalPlanFragment canonicalPlanFragment)
    {
        this.fragmentResultCacheManager = requireNonNull(fragmentResultCacheManager, "fragmentResultCacheManager is null");
        this.canonicalPlanFragment = requireNonNull(canonicalPlanFragment, "canonicalPlanFragment is null");
    }

    public FragmentResultCacheManager getFragmentResultCacheManager()
    {
        return fragmentResultCacheManager;
    }

    public CanonicalPlanFragment getCanonicalPlanFragment()
    {
        return canonicalPlanFragment;
    }
}
