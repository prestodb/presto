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

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.BasePlanFragmenter.FragmentProperties;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.finalizeSubPlan;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.getOutputTableWriterNodeIds;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final QueryManagerConfig config;
    private final PlanChecker distributedPlanChecker;
    private final PlanChecker singleNodePlanChecker;

    @Inject
    public PlanFragmenter(Metadata metadata, NodePartitioningManager nodePartitioningManager, QueryManagerConfig queryManagerConfig, FeaturesConfig featuresConfig, PlanCheckerProviderManager planCheckerProviderManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.config = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.distributedPlanChecker = new PlanChecker(requireNonNull(featuresConfig, "featuresConfig is null"), false, planCheckerProviderManager);
        this.singleNodePlanChecker = new PlanChecker(requireNonNull(featuresConfig, "featuresConfig is null"), true, planCheckerProviderManager);
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        VariableAllocator variableAllocator = new VariableAllocator(plan.getTypes().allVariables());
        return createSubPlans(session, plan, forceSingleNode, idAllocator, variableAllocator, warningCollector);
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, WarningCollector warningCollector)
    {
        Fragmenter fragmenter = new Fragmenter(
                session,
                metadata,
                plan.getStatsAndCosts(),
                forceSingleNode ? singleNodePlanChecker : distributedPlanChecker,
                warningCollector,
                idAllocator,
                variableAllocator,
                getOutputTableWriterNodeIds(plan.getRoot()));

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(
                Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                plan.getRoot().getOutputVariables()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        return finalizeSubPlan(subPlan, config, metadata, nodePartitioningManager, session, forceSingleNode, warningCollector, subPlan.getFragment().getPartitioning());
    }

    private static class Fragmenter
            extends BasePlanFragmenter
    {
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(Session session, Metadata metadata, StatsAndCosts statsAndCosts, PlanChecker planChecker, WarningCollector warningCollector, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Set<PlanNodeId> outputTableWriterNodeIds)
        {
            super(session, metadata, statsAndCosts, planChecker, warningCollector, idAllocator, variableAllocator, outputTableWriterNodeIds);
        }

        @Override
        public PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(nextFragmentId++);
        }
    }
}
