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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.BasePlanFragmenter;
import com.facebook.presto.sql.planner.BasePlanFragmenter.FragmentProperties;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.finalizeSubPlan;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.getOutputTableWriterNodeIds;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.translateOutputLayout;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * IterativePlanFragmenter creates plan fragments for all fragments
 * whose sources have finished executing according to the isFragmentFinished
 * function.  Its createReadySubPlans method will return a PlanAndFragments
 * with any new SubPlans that are ready for execution, and the remaining plan
 * with any portions already turned into fragments replaced by RemoteSourceNodes.
 */
@NotThreadSafe
public class IterativePlanFragmenter
{
    private final Function<PlanFragmentId, Boolean> isFragmentFinished;
    private final Plan originalPlan;
    private final Metadata metadata;
    private final PlanChecker planChecker;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final NodePartitioningManager nodePartitioningManager;
    private final QueryManagerConfig queryManagerConfig;
    private final Session session;
    private final WarningCollector warningCollector;
    private final boolean forceSingleNode;

    // Fragment numbers need to be unique across the whole query,
    // so keep it in this top-level class.
    // Note that the fragment numbering here will be different
    // from with the default PlanFragmenter
    // Here fragment ids will increase as you approach
    // the root instead of the other way around.
    // By convention, the root fragment will still be
    // number 0
    private int nextFragmentId = ROOT_FRAGMENT_ID + 1;
    private final Map<PlanFragmentId, SubPlan> subPlanByFragmentId = new HashMap<>();

    public IterativePlanFragmenter(
            Plan originalPlan,
            Function<PlanFragmentId, Boolean> isFragmentFinished,
            Metadata metadata,
            PlanChecker planChecker,
            PlanNodeIdAllocator idAllocator,
            NodePartitioningManager nodePartitioningManager,
            QueryManagerConfig queryManagerConfig,
            Session session,
            WarningCollector warningCollector,
            boolean forceSingleNode)
    {
        this.originalPlan = requireNonNull(originalPlan, "originalPlan is null");
        this.isFragmentFinished = requireNonNull(isFragmentFinished, "isSourceReady is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = new VariableAllocator(originalPlan.getTypes().allVariables());
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.forceSingleNode = forceSingleNode;
    }

    /**
     * @param plan the plan to generate subplans from
     * @return PlanAndFragments containing any new fragments ready for execution and the
     * remaining unfragmented plan that was not yet ready for execution
     * Any portions of the original plan that have been turned into fragments are
     * replaced with RemoteSourceNodes in the remainingPlan
     */
    public PlanAndFragments createReadySubPlans(PlanNode plan)
    {
        IterativeFragmenter iterativeFragmenter = new IterativeFragmenter(
                session,
                metadata,
                originalPlan.getStatsAndCosts(),
                planChecker,
                warningCollector,
                idAllocator,
                variableAllocator,
                getOutputTableWriterNodeIds(plan));
        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(
                Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                plan.getOutputVariables()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode planRoot = SimplePlanRewriter.rewriteWith(iterativeFragmenter, plan, properties);
        List<SubPlan> subPlans;
        Optional<PlanNode> remainingPlan;
        if (isFragmentReadyForExecution(planRoot)) {
            // if the root of the plan is ready for execution, build
            // the root fragment
            subPlans = ImmutableList.of(iterativeFragmenter.buildRootFragment(planRoot, properties));
            remainingPlan = Optional.empty();
        }
        else {
            // if the root of the plan is not ready for execution,
            // save it for reoptimization later
            subPlans = properties.getChildren();
            remainingPlan = Optional.of(planRoot);
        }

        // The initial list of subPlans will include subPlans that were created and returned during previous iterations.
        // Only return new subplans.
        subPlans = subPlans.stream().filter(subPlan -> !subPlanByFragmentId.containsKey(subPlan.getFragment().getId())).collect(toImmutableList());

        // add new subPlans to the map
        subPlans.forEach(subPlan -> subPlanByFragmentId.putIfAbsent(subPlan.getFragment().getId(), subPlan));

        // apply fragment rewrites like grouped execution tagging
        // and rewriting the partition handle
        PartitioningHandle partitioningHandle = properties.getPartitioningHandle();
        subPlans = subPlans.stream()
                .map(subPlan -> finalizeSubPlan(subPlan, queryManagerConfig, metadata, nodePartitioningManager, session, forceSingleNode, warningCollector, partitioningHandle))
                .collect(toImmutableList());

        return new PlanAndFragments(remainingPlan, subPlans);
    }

    private boolean isFragmentReadyForExecution(PlanNode node)
    {
        return node.getSources().stream().allMatch(source -> source.accept(new ExecutionReadinessChecker(), null));
    }

    /**
     * Validates whether a plan or section of plan is ready to
     * be converted to a PlanFragment and executed.  A plan is
     * ready for execution if it does not contain any remote exchanges
     * and all of its RemoteSourceNodes have finished executing
     */
    private class ExecutionReadinessChecker
            extends InternalPlanVisitor<Boolean, Void>
    {
        @Override
        public Boolean visitPlan(PlanNode node, Void context)
        {
            return node.getSources().stream()
                    .allMatch(source -> source.accept(this, context));
        }

        @Override
        public Boolean visitExchange(ExchangeNode node, Void context)
        {
            if (node.getScope() != LOCAL) {
                // previous stage has not yet executed
                return false;
            }
            return visitPlan(node, context);
        }

        @Override
        public Boolean visitRemoteSource(RemoteSourceNode node, Void context)
        {
            return node.getSourceFragmentIds().stream()
                    .allMatch(isFragmentFinished::apply);
        }
    }

    /**
     * creates SubPlans only for the parts of the plan that are
     * ready for execution.  The rest of the plan remains unfragmented
     */
    private class IterativeFragmenter
            extends BasePlanFragmenter
    {
        public IterativeFragmenter(
                Session session,
                Metadata metadata,
                StatsAndCosts statsAndCosts,
                PlanChecker planChecker,
                WarningCollector warningCollector,
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator,
                Set<PlanNodeId> outputTableWriterNodeIds)
        {
            super(session, metadata, statsAndCosts, planChecker, warningCollector, idAllocator, variableAllocator, outputTableWriterNodeIds);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getScope() != REMOTE_MATERIALIZED || isFragmentReadyForExecution(node)) {
                // create child fragments
                return super.visitExchange(node, context);
            }

            // don't fragment
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(translateOutputLayout(node.getPartitioningScheme(), node.getInputs().get(sourceIndex)));
                builder.add(context.rewrite(node.getSources().get(sourceIndex), childProperties));
                context.get().addChildren(childProperties.getChildren());
            }
            return node.replaceChildren(builder.build());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<FragmentProperties> context)
        {
            List<SubPlan> childSubPlans = node.getSourceFragmentIds().stream()
                    .map(subPlanByFragmentId::get)
                    .collect(toImmutableList());
            context.get().addChildren(childSubPlans);

            // the partitioning handle should be the same as the handle from the partitioning scheme
            // of any of the input fragments.
            setDistributionForExchange(node.getExchangeType(), childSubPlans.get(0).getFragment().getPartitioningScheme(), context);
            return super.visitRemoteSource(node, context);
        }

        @Override
        public PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(nextFragmentId++);
        }
    }

    public static class PlanAndFragments
    {
        // the remaining part of the plan that is not yet ready
        // for execution.
        private final Optional<PlanNode> remainingPlan;

        // fragments that are ready to be executed
        private final List<SubPlan> readyFragments;

        private PlanAndFragments(Optional<PlanNode> remainingPlan, List<SubPlan> readyFragments)
        {
            this.remainingPlan = requireNonNull(remainingPlan, "remainingPlan is null");
            this.readyFragments = ImmutableList.copyOf(requireNonNull(readyFragments, "readyFragments is null"));
        }

        public Optional<PlanNode> getRemainingPlan()
        {
            return remainingPlan;
        }

        public boolean hasRemainingPlan()
        {
            return remainingPlan.isPresent();
        }

        public List<SubPlan> getReadyFragments()
        {
            return readyFragments;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(remainingPlan, readyFragments);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            PlanAndFragments other = (PlanAndFragments) obj;
            return Objects.equals(this.remainingPlan, other.remainingPlan) && Objects.equals(this.readyFragments, other.readyFragments);
        }
    }
}
