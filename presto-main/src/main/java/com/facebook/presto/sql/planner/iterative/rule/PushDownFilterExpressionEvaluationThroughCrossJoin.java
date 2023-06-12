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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getPushdownFilterExpressionEvaluationThroughCrossJoinStrategy;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractDisjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PushDownFilterThroughCrossJoinStrategy.DISABLED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PushDownFilterThroughCrossJoinStrategy.REWRITTEN_TO_INNER_JOIN;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.rewriteExpressionWithCSE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.iterative.rule.CrossJoinWithArrayContainsToInnerJoin.getCandidateArrayContainsExpression;
import static com.facebook.presto.sql.planner.iterative.rule.CrossJoinWithOrFilterToInnerJoin.getCandidateOrExpression;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Stream.concat;

/**
 * Output of cross join is larger than input, push down expression evaluation can save calculation cost.
 * <pre>
 *     - Filter l_key1 = cardinality(r_key1)
 *          - Cross Join
 *              - scan l
 *              - scan r
 * </pre>
 * to
 * <pre>
 *     - Filter l_key1 = card
 *          - Cross Join
 *              - scan l
 *              - project
 *                  card := cardinality(r_key1)
 *                  - scan r
 * </pre>
 */
public class PushDownFilterExpressionEvaluationThroughCrossJoin
        implements Rule<FilterNode>
{
    private static final String CONTAINS_FUNCTION_NAME = "presto.default.contains";
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().matching(x -> x.getCriteria().isEmpty() && x.getType().equals(JoinNode.Type.INNER)).capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public PushDownFilterExpressionEvaluationThroughCrossJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    private static boolean canRewrittenToInnerJoin(RowExpression filter, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        return getCandidateOrExpression(filter, left, right) != null || getCandidateArrayContainsExpression(filter, left, right) != null;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return !getPushdownFilterExpressionEvaluationThroughCrossJoinStrategy(session).equals(DISABLED);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        List<Set<RowExpression>> rowExpressionToProject = getRowExpressions(filterNode.getPredicate(), joinNode.getLeft().getOutputVariables(), joinNode.getRight().getOutputVariables());
        if (rowExpressionToProject.stream().allMatch(x -> x.isEmpty())) {
            return Result.empty();
        }
        Map<RowExpression, VariableReferenceExpression> rewrittenExpressionMap = concat(rowExpressionToProject.get(0).stream(), rowExpressionToProject.get(1).stream())
                .collect(toImmutableMap(identity(), x -> context.getVariableAllocator().newVariable(x)));
        RowExpression rewrittenFilter = rewriteExpressionWithCSE(filterNode.getPredicate(), rewrittenExpressionMap);

        Map<VariableReferenceExpression, RowExpression> leftAssignment = rowExpressionToProject.get(0).stream().collect(toImmutableMap(x -> rewrittenExpressionMap.get(x), identity()));
        Map<VariableReferenceExpression, RowExpression> rightAssignment = rowExpressionToProject.get(1).stream().collect(toImmutableMap(x -> rewrittenExpressionMap.get(x), identity()));

        PlanNode leftInput = joinNode.getLeft();
        if (!leftAssignment.isEmpty()) {
            leftInput = PlannerUtils.addProjections(joinNode.getLeft(), context.getIdAllocator(), leftAssignment);
        }
        PlanNode rightInput = joinNode.getRight();
        if (!rightAssignment.isEmpty()) {
            rightInput = PlannerUtils.addProjections(joinNode.getRight(), context.getIdAllocator(), rightAssignment);
        }

        // Only enable if the cross join can be rewritten to inner join after the rewrite
        if (getPushdownFilterExpressionEvaluationThroughCrossJoinStrategy(context.getSession()).equals(REWRITTEN_TO_INNER_JOIN)
                && !canRewrittenToInnerJoin(rewrittenFilter, leftInput.getOutputVariables(), rightInput.getOutputVariables())) {
            return Result.empty();
        }

        Assignments.Builder identity = Assignments.builder();
        identity.putAll(filterNode.getOutputVariables().stream().collect(toImmutableMap(identity(), identity())));
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                filterNode.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                new JoinNode(
                                        joinNode.getSourceLocation(),
                                        context.getIdAllocator().getNextId(),
                                        joinNode.getType(),
                                        leftInput,
                                        rightInput,
                                        joinNode.getCriteria(),
                                        ImmutableList.<VariableReferenceExpression>builder()
                                                .addAll(leftInput.getOutputVariables())
                                                .addAll(rightInput.getOutputVariables())
                                                .build(),
                                        joinNode.getFilter(),
                                        joinNode.getLeftHashVariable(),
                                        joinNode.getRightHashVariable(),
                                        joinNode.getDistributionType(),
                                        joinNode.getDynamicFilters()),
                                rewrittenFilter),
                        identity.build()));
    }

    // TODO: this function only works for filter in form of or condition and array contains function etc. make it generic to work for all RowExpressions
    private List<Set<RowExpression>> getRowExpressions(RowExpression filterPredicate, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        List<Set<RowExpression>> candidateFromOrCondition = getRowExpressionsFromOrCondition(filterPredicate, left, right);
        List<Set<RowExpression>> candidateFromArrayContains = getRowExpressionsFromArrayContains(filterPredicate, left, right);

        ImmutableSet.Builder<RowExpression> leftCandidate = ImmutableSet.builder();
        leftCandidate.addAll(candidateFromOrCondition.get(0));
        leftCandidate.addAll(candidateFromArrayContains.get(0));

        ImmutableSet.Builder<RowExpression> rightCandidate = ImmutableSet.builder();
        rightCandidate.addAll(candidateFromOrCondition.get(1));
        rightCandidate.addAll(candidateFromArrayContains.get(1));

        return ImmutableList.of(leftCandidate.build(), rightCandidate.build());
    }

    private List<Set<RowExpression>> getRowExpressionsFromOrCondition(RowExpression filterPredicate, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        Set<RowExpression> leftRowExpression = new HashSet<>();
        Set<RowExpression> rightRowExpression = new HashSet<>();
        for (RowExpression conjunct : extractConjuncts(filterPredicate)) {
            for (RowExpression disjunct : extractDisjuncts(conjunct)) {
                if (disjunct instanceof CallExpression && ((CallExpression) disjunct).getDisplayName().equals("EQUAL")) {
                    CallExpression callExpression = (CallExpression) disjunct;
                    addCandidateExpression(callExpression.getArguments().get(0), left, right, leftRowExpression, rightRowExpression);
                    addCandidateExpression(callExpression.getArguments().get(1), left, right, leftRowExpression, rightRowExpression);
                }
            }
        }
        return ImmutableList.of(leftRowExpression, rightRowExpression);
    }

    private List<Set<RowExpression>> getRowExpressionsFromArrayContains(RowExpression filterPredicate, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        Set<RowExpression> leftRowExpression = new HashSet<>();
        Set<RowExpression> rightRowExpression = new HashSet<>();
        if (filterPredicate instanceof CallExpression && ((CallExpression) filterPredicate).getFunctionHandle().getName().equals(CONTAINS_FUNCTION_NAME)) {
            CallExpression callExpression = (CallExpression) filterPredicate;
            addCandidateExpression(callExpression.getArguments().get(0), left, right, leftRowExpression, rightRowExpression);
            addCandidateExpression(callExpression.getArguments().get(1), left, right, leftRowExpression, rightRowExpression);
        }
        return ImmutableList.of(leftRowExpression, rightRowExpression);
    }

    private void addCandidateExpression(RowExpression candidate, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right, Set<RowExpression> leftRowExpression, Set<RowExpression> rightRowExpression)
    {
        List<VariableReferenceExpression> variablesInExpression = extractAll(candidate);
        if (!variablesInExpression.isEmpty() && determinismEvaluator.isDeterministic(candidate) && !(candidate instanceof VariableReferenceExpression)) {
            if (left.containsAll(variablesInExpression)) {
                leftRowExpression.add(candidate);
            }
            else if (right.containsAll(variablesInExpression)) {
                rightRowExpression.add(candidate);
            }
        }
    }
}
