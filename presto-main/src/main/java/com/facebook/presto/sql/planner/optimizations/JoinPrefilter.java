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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isJoinPrefilterEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.orNullHashCode;
import static com.facebook.presto.sql.planner.PlannerUtils.projectExpressions;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.callOperator;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer filter the right side of a join with the unique join keys on the left side of the join. When the join key is wide or
 * there are multiple join keys, we are to do filter on the hash instead of using the keys.
 * It will convert plan from
 * <pre>
 *     - InnerJoin
 *          leftKey = rightKey
 *          - scan l
 *          - scan r
 * </pre>
 * into
 * <pre>
 *     - InnerJoin
 *          leftKey = rightKey
 *          - scan l
 *          - semiJoin
 *              r.rightKey in l.leftKey
 *              - scan r
 *              - distinct aggregation
 *                  group by leftKey
 *                  - scan l
 * </pre>
 * And for join with varchar type
 * <pre>
 *     - InnerJoin
 *          leftKey (varchar) = rightKey (varchar)
 *          - scan l
 *          - scan r
 * </pre>
 * into
 * <pre>
 *     - InnerJoin
 *          leftKey (varchar) = rightKey (varchar)
 *          - scan l
 *          - semiJoin
 *              r.rightKeyHash in l.leftKeyHash
 *              - project
 *                  r.rightKeyHash = xx_hash64(r.rightKey)
 *                  - scan r
 *              - distinct aggregation
 *                  group by leftKeyHash
 *                  - project
 *                      l.leftKeyHash = xx_hash64(l.leftKey)
 *                      - scan l
 * </pre>
 * And for join with multiple keys
 * <pre>
 *     - InnerJoin
 *          leftKey1 = rightKey1 and leftKey2 = rightKey2
 *          - scan l
 *          - scan r
 * </pre>
 * into
 * <pre>
 *     - InnerJoin
 *          leftKey1 = rightKey1 and leftKey2 = rightKey2
 *          - scan l
 *          - semiJoin
 *              r.rightKeysHash in l.leftKeysHash
 *              - project
 *                  r.rightKeysHash = combine_hash(xx_hash64(rightKey1), xx_hash64(rightKey2))
 *                  - scan r
 *              - distinct aggregation
 *                  group by leftKeysHash
 *                  - project
 *                      l.leftKeysHash = combine_hash(xx_hash64(leftKey1), xx_hash64(leftKey2))
 *                      - scan l
 * </pre>
 */

public class JoinPrefilter
        implements PlanOptimizer
{
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public JoinPrefilter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isJoinPrefilterEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            Rewriter rewriter = new Rewriter(session, metadata, idAllocator, variableAllocator, metadata.getFunctionAndTypeManager());
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "functionAndTypeManager is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "idAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            PlanNode rewrittenLeft = rewriteWith(this, left);
            PlanNode rewrittenRight = rewriteWith(this, right);
            List<EquiJoinClause> equiJoinClause = node.getCriteria();

            // We apply this for only left and inner join and the left side of the join is a simple scan
            if ((node.getType() == LEFT || node.getType() == INNER) && isScanFilterProject(rewrittenLeft) && !node.getCriteria().isEmpty()) {
                List<VariableReferenceExpression> leftKeyList = equiJoinClause.stream().map(EquiJoinClause::getLeft).collect(toImmutableList());
                List<VariableReferenceExpression> rightKeyList = equiJoinClause.stream().map(EquiJoinClause::getRight).collect(toImmutableList());
                checkState(IntStream.range(0, leftKeyList.size()).boxed().allMatch(i -> leftKeyList.get(i).getType().equals(rightKeyList.get(i).getType())));

                boolean hashJoinKey = leftKeyList.size() > 1 || (leftKeyList.get(0).getType().equals(VARCHAR) || leftKeyList.get(0).getType() instanceof VarcharType);

                // First create a SELECT DISTINCT leftKey FROM left
                Map<VariableReferenceExpression, VariableReferenceExpression> leftVarMap = new HashMap();
                PlanNode leftKeys = clonePlanNode(rewrittenLeft, session, metadata, idAllocator, leftKeyList, leftVarMap);
                ImmutableList.Builder<RowExpression> expressionsToProject = ImmutableList.builder();
                if (hashJoinKey) {
                    RowExpression hashExpression = getVariableHash(leftKeyList);
                    expressionsToProject.add(hashExpression);
                }
                else {
                    expressionsToProject.add(leftVarMap.get(leftKeyList.get(0)));
                }
                PlanNode projectNode = projectExpressions(leftKeys, idAllocator, variableAllocator, expressionsToProject.build(), ImmutableList.of());

                VariableReferenceExpression rightKeyToFilter = rightKeyList.get(0);
                if (hashJoinKey) {
                    RowExpression hashExpression = getVariableHash(rightKeyList);
                    rightKeyToFilter = variableAllocator.newVariable(hashExpression);
                    rewrittenRight = addProjections(rewrittenRight, idAllocator, ImmutableMap.of(rightKeyToFilter, hashExpression));
                }

                // DISTINCT on the leftkey or hash if wide column
                PlanNode filteringSource = new AggregationNode(
                        node.getLeft().getSourceLocation(),
                        idAllocator.getNextId(),
                        projectNode,
                        ImmutableMap.of(),
                        singleGroupingSet(projectNode.getOutputVariables()),
                        projectNode.getOutputVariables(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());

                // There should be only one output variable. Project that
                filteringSource = projectExpressions(filteringSource, idAllocator, variableAllocator, ImmutableList.of(filteringSource.getOutputVariables().get(0)), ImmutableList.of());

                // Now we add a semijoin as the right side
                VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semiJoinOutput", BOOLEAN);
                SemiJoinNode semiJoinNode = new SemiJoinNode(
                        node.getRight().getSourceLocation(),
                        idAllocator.getNextId(),
                        node.getStatsEquivalentPlanNode(),
                        rewrittenRight,
                        filteringSource,
                        rightKeyToFilter,
                        filteringSource.getOutputVariables().get(0),
                        semiJoinOutput,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of());

                rewrittenRight = new FilterNode(semiJoinNode.getSourceLocation(), idAllocator.getNextId(), semiJoinNode, semiJoinOutput);
                if (rewrittenRight.getOutputVariables().size() > node.getRight().getOutputVariables().size()) {
                    rewrittenRight = restrictOutput(rewrittenRight, idAllocator, node.getRight().getOutputVariables());
                }
            }

            if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                planChanged = true;
                return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
            }

            return node;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        private RowExpression getVariableHash(List<VariableReferenceExpression> inputVariables)
        {
            List<CallExpression> hashExpressionList = inputVariables.stream().map(keyVariable ->
                    callOperator(functionAndTypeManager.getFunctionAndTypeResolver(), OperatorType.XX_HASH_64, BIGINT, keyVariable)).collect(toImmutableList());
            RowExpression hashExpression = hashExpressionList.get(0);
            if (hashExpressionList.size() > 1) {
                hashExpression = orNullHashCode(hashExpression);
                for (int i = 1; i < hashExpressionList.size(); ++i) {
                    hashExpression = call(functionAndTypeManager, "combine_hash", BIGINT, hashExpression, orNullHashCode(hashExpressionList.get(i)));
                }
            }
            return hashExpression;
        }
    }
}
