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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getRandomizeOuterJoinNullKeyStrategy;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeOuterJoinNullKeyStrategy;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeOuterJoinNullKeyStrategy.ALWAYS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeOuterJoinNullKeyStrategy.DISABLED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeOuterJoinNullKeyStrategy.KEY_FROM_OUTER_JOIN;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Randomize the join key of outer joins if the join key has many NULL values so as to mitigate skew.
 * The optimization has three strategies, `DISABLED` to skip the optimization, `ALWAYS` to always enabled for all outer joins, and `KEY_FROM_OUTER_JOIN` to enable only when the
 * join key is from the outer side of a outer join which is likely to have many NULLs.
 *
 * When Strategy is `KEY_FROM_OUTER_JOIN`:
 * <pre>
 * - LeftJoin
 *          l.key = r.key
 *      - Project
 *          l.key = T2.v
 *          - LeftJoin
 *              T1.k = T2.k
 *              - Scan T1(k, v)
 *              - Scan T2(k, v)
 *      - Project
 *          r.key = T3.k
 *          - Scan T3(k)
 * </pre>
 * to
 * <pre>
 * - LeftJoin
 *          l.key = r.key
 *      - Project
 *          l.key = COALESCE(T2.v, Randomize(T2.v))
 *          - LeftJoin
 *              T1.k = T2.k
 *              - Scan T1(k, v)
 *              - Scan T2(k, v)
 *      - Project
 *          r.key = COALESCE(T3.k, Randomize(T3.k))
 *          - Scan T3(k)
 * </pre>
 *
 * When Strategy is `ALWAYS`:
 * <pre>
 * - LeftJoin
 *          l.key = r.key
 *      - Project
 *          l.key = T2.v
 *          - LeftJoin
 *              T1.k = T2.k
 *              - Scan T1(k, v)
 *              - Scan T2(k, v)
 *      - Project
 *          r.key = T3.k
 *          - Scan T3(k)
 * </pre>
 * to
 * <pre>
 * - LeftJoin
 *          l.key = r.key
 *      - Project
 *          l.key = COALESCE(T2.v, Randomize(T2.v))
 *          - LeftJoin
 *              T1.randK = T2.randK
 *              - Project
 *                  T1.randK = COALESCE(T1.k, Randomize(T1.k))
 *                  - Scan T1(k, v)
 *              - Project
 *                  T2.randK = COALESCE(T2.k, Randomize(T2.k))
 *                  - Scan T2(k, v)
 *      - Project
 *          r.key = COALESCE(T3.k, Randomize(T3.k))
 *          - Scan T3(k)
 * </pre>
 */

public class RandomizeNullKeyInOuterJoin
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public RandomizeNullKeyInOuterJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (getJoinDistributionType(session).canPartition() && !getRandomizeOuterJoinNullKeyStrategy(session).equals(DISABLED)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session, functionAndTypeManager, idAllocator, variableAllocator), plan, new HashSet<>());
        }

        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
    {
        private static final String LEFT_PREFIX = "l";
        private static final String RIGHT_PREFIX = "r";
        private final Session session;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final VariableAllocator planVariableAllocator;
        private final Map<String, Map<VariableReferenceExpression, VariableReferenceExpression>> keyToRandomKeyMap;
        private final RandomizeOuterJoinNullKeyStrategy strategy;

        private Rewriter(Session session,
                FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator planVariableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.planVariableAllocator = requireNonNull(planVariableAllocator, "planVariableAllocator is null");
            this.keyToRandomKeyMap = new HashMap<>();
            this.strategy = getRandomizeOuterJoinNullKeyStrategy(session);
        }

        private static boolean isSupportedType(VariableReferenceExpression variable)
        {
            return Stream.of(BIGINT, DATE).anyMatch(x -> x.equals(variable.getType())) || (variable.getType() instanceof VarcharType);
        }

        private static boolean isPartitionedJoin(JoinNode joinNode)
        {
            return joinNode.getDistributionType().isPresent() && joinNode.getDistributionType().get() == PARTITIONED;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (strategy.equals(KEY_FROM_OUTER_JOIN)) {
                ImmutableMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> renameAssignmentBuilder = ImmutableMultimap.builder();
                node.getAssignments().getMap().forEach((k, v) -> {
                    if (v instanceof VariableReferenceExpression) {
                        renameAssignmentBuilder.put((VariableReferenceExpression) v, k);
                    }
                });
                ImmutableMultimap<VariableReferenceExpression, VariableReferenceExpression> renameAssignment = renameAssignmentBuilder.build();
                context.get().addAll(context.get().stream().flatMap(x -> renameAssignment.get(x).stream()).collect(toImmutableSet()));
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // In this optimization, we add the randomized key to output so it can be reused in case the same key is used in outer join in later stage.
            // However this can change the input for a cross join (if the input of the cross join gets optimized). For cross join, it does not allow mismatch
            // of input and output. Need to reassign output in case input of the cross join changes.
            if (joinNode.isCrossJoin()) {
                PlanNode rewrittenLeft = context.rewrite(joinNode.getLeft(), context.get());
                PlanNode rewrittenRight = context.rewrite(joinNode.getRight(), context.get());
                Set<VariableReferenceExpression> inputVariables = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(rewrittenLeft.getOutputVariables())
                        .addAll(rewrittenRight.getOutputVariables())
                        .build();
                checkState(inputVariables.containsAll(joinNode.getOutputVariables()));
                return new JoinNode(
                        joinNode.getSourceLocation(),
                        joinNode.getId(),
                        joinNode.getStatsEquivalentPlanNode(),
                        joinNode.getType(),
                        rewrittenLeft,
                        rewrittenRight,
                        joinNode.getCriteria(),
                        inputVariables.stream().collect(toImmutableList()),
                        joinNode.getFilter(),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilters());
            }

            // Only do optimization for outer join and partitioned join
            if (Stream.of(LEFT, RIGHT, FULL).noneMatch(joinType -> joinType.equals(joinNode.getType())) || !isPartitionedJoin(joinNode)) {
                PlanNode result = context.defaultRewrite(joinNode, context.get());
                return result;
            }

            PlanNode rewrittenLeft = context.rewrite(joinNode.getLeft(), context.get());
            PlanNode rewrittenRight = context.rewrite(joinNode.getRight(), context.get());

            List<JoinNode.EquiJoinClause> candidateEquiJoinClauses = joinNode.getCriteria().stream()
                    .filter(x -> isSupportedType(x.getLeft()) && isSupportedType(x.getRight()))
                    .filter(x -> strategy.equals(ALWAYS) || (strategy.equals(KEY_FROM_OUTER_JOIN) && (context.get().contains(x.getLeft()) || context.get().contains(x.getRight()))))
                    .collect(toImmutableList());
            if (candidateEquiJoinClauses.isEmpty()) {
                PlanNode result = replaceChildren(joinNode, ImmutableList.of(rewrittenLeft, rewrittenRight));
                if (strategy.equals(KEY_FROM_OUTER_JOIN)) {
                    checkState(result instanceof JoinNode);
                    updateCandidates((JoinNode) result, context);
                }
                return result;
            }

            List<VariableReferenceExpression> leftJoinKeys = candidateEquiJoinClauses.stream()
                    .map(x -> x.getLeft())
                    .filter(x -> !isAlreadyRandomized(rewrittenLeft, x, LEFT_PREFIX))
                    .collect(toImmutableList());
            Map<VariableReferenceExpression, RowExpression> leftKeyRandomVariableMap = generateRandomKeyMap(leftJoinKeys, LEFT_PREFIX);

            List<VariableReferenceExpression> rightJoinKeys = candidateEquiJoinClauses.stream()
                    .map(x -> x.getRight())
                    .filter(x -> !isAlreadyRandomized(rewrittenRight, x, RIGHT_PREFIX))
                    .collect(toImmutableList());
            Map<VariableReferenceExpression, RowExpression> rightKeyRandomVariableMap = generateRandomKeyMap(rightJoinKeys, RIGHT_PREFIX);

            ImmutableList.Builder<JoinNode.EquiJoinClause> joinClauseBuilder = ImmutableList.builder();
            // Rewrite supported join clauses
            List<JoinNode.EquiJoinClause> rewrittenJoinClauses = candidateEquiJoinClauses.stream()
                    .map(x -> new JoinNode.EquiJoinClause(keyToRandomKeyMap.get(LEFT_PREFIX).get(x.getLeft()), keyToRandomKeyMap.get(RIGHT_PREFIX).get(x.getRight())))
                    .collect(toImmutableList());
            joinClauseBuilder.addAll(rewrittenJoinClauses);
            // Add the join clauses which are not supported back
            List<JoinNode.EquiJoinClause> unchangedJoinClauses = joinNode.getCriteria().stream()
                    .filter(x -> !candidateEquiJoinClauses.contains(x))
                    .collect(toImmutableList());
            joinClauseBuilder.addAll(unchangedJoinClauses);

            Assignments.Builder leftAssignments = Assignments.builder();
            leftAssignments.putAll(leftKeyRandomVariableMap);
            leftAssignments.putAll(rewrittenLeft.getOutputVariables().stream().collect(toImmutableMap(identity(), identity())));

            Assignments.Builder rightAssignments = Assignments.builder();
            rightAssignments.putAll(rightKeyRandomVariableMap);
            rightAssignments.putAll(rewrittenRight.getOutputVariables().stream().collect(toImmutableMap(identity(), identity())));

            ImmutableList.Builder<VariableReferenceExpression> joinOutputBuilder = ImmutableList.builder();
            joinOutputBuilder.addAll(leftKeyRandomVariableMap.keySet());
            joinOutputBuilder.addAll(rightKeyRandomVariableMap.keySet());
            joinOutputBuilder.addAll(joinNode.getOutputVariables());

            session.getOptimizerInformationCollector().addInformation(
                    new PlanOptimizerInformation(RandomizeNullKeyInOuterJoin.class.getSimpleName(), true, Optional.empty()));
            JoinNode newJoinNode = new JoinNode(
                    joinNode.getSourceLocation(),
                    joinNode.getId(),
                    joinNode.getStatsEquivalentPlanNode(),
                    joinNode.getType(),
                    new ProjectNode(rewrittenLeft.getSourceLocation(), planNodeIdAllocator.getNextId(), rewrittenLeft, leftAssignments.build(), LOCAL),
                    new ProjectNode(rewrittenRight.getSourceLocation(), planNodeIdAllocator.getNextId(), rewrittenRight, rightAssignments.build(), LOCAL),
                    joinClauseBuilder.build(),
                    joinOutputBuilder.build(),
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters());
            if (strategy.equals(KEY_FROM_OUTER_JOIN)) {
                updateCandidates(newJoinNode, context);
            }
            return newJoinNode;
        }

        private RowExpression randomizeJoinKey(RowExpression keyExpression, String prefix)
        {
            int partitionCount = getHashPartitionCount(session);
            RowExpression randomNumber = call(
                    functionAndTypeManager,
                    "random",
                    BIGINT,
                    constant((long) partitionCount, BIGINT));
            RowExpression randomNumberVarchar = call("CAST", functionAndTypeManager.lookupCast(CAST, randomNumber.getType(), VARCHAR), VARCHAR, randomNumber);
            RowExpression concatExpression = call(functionAndTypeManager,
                    "concat",
                    VARCHAR,
                    ImmutableList.of(constant(Slices.utf8Slice(prefix), VARCHAR), randomNumberVarchar));

            RowExpression castToVarchar = keyExpression;
            // Only do cast if keyExpression is not VARCHAR type.
            if (!(keyExpression.getType() instanceof VarcharType)) {
                castToVarchar = call("CAST", functionAndTypeManager.lookupCast(CAST, keyExpression.getType(), VARCHAR), VARCHAR, keyExpression);
            }
            return new SpecialFormExpression(COALESCE, VARCHAR, ImmutableList.of(castToVarchar, concatExpression));
        }

        // Do not need to generate randomized variable if the joinKey 1) has already been randomized with the same prefix 2) included in the output of the source node
        private boolean isAlreadyRandomized(PlanNode source, VariableReferenceExpression joinKey, String prefix)
        {
            return keyToRandomKeyMap.containsKey(prefix) && keyToRandomKeyMap.get(prefix).containsKey(joinKey) && source.getOutputVariables().contains(keyToRandomKeyMap.get(prefix).get(joinKey));
        }

        private Map<VariableReferenceExpression, RowExpression> generateRandomKeyMap(List<VariableReferenceExpression> joinKeys, String prefix)
        {
            List<RowExpression> randomExpressions = joinKeys.stream().map(x -> randomizeJoinKey(x, prefix)).collect(toImmutableList());
            List<VariableReferenceExpression> randomVariable = randomExpressions.stream()
                    .map(x -> planVariableAllocator.newVariable(x, RandomizeNullKeyInOuterJoin.class.getSimpleName()))
                    .collect(toImmutableList());

            checkState(joinKeys.size() == randomVariable.size());
            Map<VariableReferenceExpression, VariableReferenceExpression> newKeyToRandomMap = IntStream.range(0, joinKeys.size()).boxed()
                    .collect(toImmutableMap(joinKeys::get, randomVariable::get));
            if (!keyToRandomKeyMap.containsKey(prefix)) {
                keyToRandomKeyMap.put(prefix, new HashMap<>());
            }
            keyToRandomKeyMap.get(prefix).putAll(newKeyToRandomMap);

            Map<VariableReferenceExpression, RowExpression> result = IntStream.range(0, randomVariable.size()).boxed()
                    .collect(toImmutableMap(randomVariable::get, randomExpressions::get));

            return result;
        }

        private void updateCandidates(JoinNode joinNode, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (joinNode.getType().equals(LEFT)) {
                context.get().addAll(joinNode.getOutputVariables().stream().filter(x -> joinNode.getRight().getOutputVariables().contains(x)).collect(toImmutableSet()));
            }
            else if (joinNode.getType().equals(RIGHT)) {
                context.get().addAll(joinNode.getOutputVariables().stream().filter(x -> joinNode.getLeft().getOutputVariables().contains(x)).collect(toImmutableSet()));
            }
            else {
                checkState(joinNode.getType().equals(FULL));
                context.get().addAll(joinNode.getOutputVariables());
            }
        }
    }
}
