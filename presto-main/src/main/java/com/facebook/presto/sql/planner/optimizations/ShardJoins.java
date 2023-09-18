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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinShardCount;
import static com.facebook.presto.SystemSessionProperties.getShardedJoinStrategy;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ShardedJoinStrategy.ALWAYS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ShardedJoinStrategy.COST_BASED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ShardedJoinStrategy.DISABLED;
import static com.facebook.presto.sql.planner.PlannerUtils.isBroadcastJoin;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Shard joins to eliminate skew:
 *
 * Transform
 * <pre>
 * - Join
 *      S.key = T.key
 *      - S
 *      - T
 * </pre>
 * to
 * <pre>
 * - Join
 *  *    S.key = T.key and leftShard = rightShard
 *  *      - Project(leftShard:=random(NumShards))
 *             - S
 *  *      - Unnest(rightShard, seq)
 *             Project(seq:=sequence(0, NumShards - 1))
 *                - T
 * </pre>
 *
 */

public class ShardJoins
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final StatsCalculator statsCalculator;
    private boolean isEnabledForTesting;

    public ShardJoins(Metadata metadata, FunctionAndTypeManager functionAndTypeManager, StatsCalculator statsCalculator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || !getShardedJoinStrategy(session).equals(DISABLED);
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
        private Rewriter(Session session, Metadata metadata,
                FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator planVariableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.planVariableAllocator = requireNonNull(planVariableAllocator, "planVariableAllocator is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (isApplicable(joinNode)) {
                long numShards = getNumberOfShards();
                RowExpression randomNumber = call(
                        functionAndTypeManager,
                        "random",
                        BIGINT,
                        constant(numShards, BIGINT));
                VariableReferenceExpression leftShardVariable = planVariableAllocator.newVariable("shard", BIGINT);
                VariableReferenceExpression rightShardVariable = planVariableAllocator.newVariable("shard", BIGINT);

                PlanNode newLeftChild = PlannerUtils.addProjections(joinNode.getLeft(), planNodeIdAllocator, planVariableAllocator, ImmutableList.of(randomNumber), ImmutableList.of(leftShardVariable));

                PlanNode newRightChild = shardInput(numShards, joinNode.getRight(), rightShardVariable);
                JoinNode.EquiJoinClause shardEquality = new JoinNode.EquiJoinClause(leftShardVariable, rightShardVariable);
                List<JoinNode.EquiJoinClause> joinCriteria = new ArrayList<>();
                joinCriteria.addAll(joinNode.getCriteria());
                joinCriteria.add(shardEquality);
                PlanNode result = new JoinNode(
                        joinNode.getSourceLocation(),
                        joinNode.getId(),
                        joinNode.getStatsEquivalentPlanNode(),
                        joinNode.getType(),
                        newLeftChild,
                        newRightChild,
                        joinCriteria,
                        joinNode.getOutputVariables(),
                        joinNode.getFilter(),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilters());

                return context.defaultRewrite(result);
            }

            return context.defaultRewrite(joinNode);
        }

        private boolean isApplicable(JoinNode joinNode)
        {
            return joinNode.getType() != FULL && joinNode.getType() != RIGHT && !isBroadcastJoin(joinNode) &&
                    (getShardedJoinStrategy(session).equals(ALWAYS) ||
                        getShardedJoinStrategy(session).equals(COST_BASED) && shouldShardJoin(joinNode));
        }

        private boolean shouldShardJoin(JoinNode joinNode)
        {
            // TODO: implement based on HBO stats
            return false;
        }

        private PlanNode shardInput(long numShards, PlanNode source, VariableReferenceExpression shardVariable)
        {
            checkState(numShards > 1);

            RowExpression sequenceExpression = call(
                    functionAndTypeManager,
                    "sequence",
                    new ArrayType(BIGINT),
                    constant((long) 0, BIGINT),
                    constant((long) numShards - 1, BIGINT));

            VariableReferenceExpression sequenceVariable = planVariableAllocator.newVariable(sequenceExpression);
            PlanNode projectSequence = PlannerUtils.addProjections(source, planNodeIdAllocator, planVariableAllocator, ImmutableList.of(sequenceExpression), ImmutableList.of(sequenceVariable));
            UnnestNode unnest = new UnnestNode(source.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    projectSequence,
                    projectSequence.getOutputVariables(),
                    ImmutableMap.of(sequenceVariable, ImmutableList.of(shardVariable)),
                    Optional.empty());
            return unnest;
        }

        private int getNumberOfShards()
        {
            // TODO: compute number of shards based on stats
            return getJoinShardCount(session);
        }
    }
}
