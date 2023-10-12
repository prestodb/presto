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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getRandomizeProbeSideStrategy;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeProbeSideStrategy.DISABLED;
import static com.facebook.presto.sql.planner.PlannerUtils.concatVariableLists;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static java.util.Objects.requireNonNull;

/**
 * Randomize the skewed side of a join by transforming:
 *     join (REPLICATED)
 *       - scan S
 *       - scan T
 *
 * into
 *
 *     join (REPLICATED)
 *       - Exchange(RoundRobin)
 *         - scan S
 *       - scan T
 *
 */

public class RandomShuffleProbeSide
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public RandomShuffleProbeSide(Metadata metadata, FunctionAndTypeManager functionAndTypeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || !getRandomizeProbeSideStrategy(session).equals(DISABLED);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata, functionAndTypeManager, idAllocator, variableAllocator), plan, new HashSet<>());
        }

        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
    {
        private final Session session;
        private final Metadata metadata;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final VariableAllocator planVariableAllocator;
        private final FeaturesConfig.RandomizeProbeSideStrategy strategy;

        private Rewriter(Session session, Metadata metadata,
                FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator planVariableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.planVariableAllocator = requireNonNull(planVariableAllocator, "planVariableAllocator is null");
            this.strategy = getRandomizeProbeSideStrategy(session);
        }

        private static boolean isBroadcastJoin(JoinNode joinNode)
        {
            return joinNode.getDistributionType().isPresent() && joinNode.getDistributionType().get() == REPLICATED;
        }

        private static boolean isApplicable(JoinNode joinNode, FeaturesConfig.RandomizeProbeSideStrategy strategy)
        {
            return isBroadcastJoin(joinNode) &&
                    (strategy == FeaturesConfig.RandomizeProbeSideStrategy.ALWAYS ||
                            strategy == FeaturesConfig.RandomizeProbeSideStrategy.COST_BASED && isLeftSideSkewed(joinNode));
        }

        private static boolean isLeftSideSkewed(JoinNode joinNode)
        {
            // TODO: implement once we enable HBO for this optimization
            return false;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!isApplicable(joinNode, strategy)) {
                return context.defaultRewrite(joinNode, context.get());
            }

            PlanNode leftChild = joinNode.getLeft();

            PlanNode newLeftChild = addRandomShuffle(leftChild);

            List<VariableReferenceExpression> outputVariables = joinNode.getOutputVariables();
            List<VariableReferenceExpression> newOutputVariables = concatVariableLists(newLeftChild.getOutputVariables(), joinNode.getRight().getOutputVariables());
            JoinNode newJoinNode = new JoinNode(
                    joinNode.getSourceLocation(),
                    joinNode.getId(),
                    joinNode.getStatsEquivalentPlanNode(),
                    joinNode.getType(),
                    newLeftChild,
                    joinNode.getRight(),
                    joinNode.getCriteria(),
                    newOutputVariables,
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters());
            return restrictOutput(newJoinNode, planNodeIdAllocator, outputVariables);
        }

        private PlanNode addRandomShuffle(PlanNode plan)
        {
            return roundRobinExchange(planNodeIdAllocator.getNextId(), REMOTE_STREAMING, plan);
        }
    }
}
