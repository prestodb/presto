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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractDisjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.rewriteExpressionWithCSE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
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
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().matching(x -> x.getCriteria().isEmpty() && x.getType().equals(JoinNode.Type.INNER)).capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public PushDownFilterExpressionEvaluationThroughCrossJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return true;
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

    // TODO: this function only works for filter in form of and(or(exp1 = exp2, exp3 = exp4), or(exp5 = exp6, exp7=exp8)) etc. make it generic to work for all RowExpressions
    private List<Set<RowExpression>> getRowExpressions(RowExpression filterPredicate, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        Set<RowExpression> leftRowExpression = new HashSet<>();
        Set<RowExpression> rightRowExpression = new HashSet<>();
        RowExpressionDeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        for (RowExpression conjunct : extractConjuncts(filterPredicate)) {
            for (RowExpression disjunct : extractDisjuncts(conjunct)) {
                if (disjunct instanceof CallExpression && ((CallExpression) disjunct).getDisplayName().equals("EQUAL")) {
                    CallExpression callExpression = (CallExpression) disjunct;
                    RowExpression argument0 = callExpression.getArguments().get(0);
                    RowExpression argument1 = callExpression.getArguments().get(1);

                    List<VariableReferenceExpression> variablesInArgument0 = extractAll(argument0);
                    if (!variablesInArgument0.isEmpty() && determinismEvaluator.isDeterministic(argument0) && !(argument0 instanceof VariableReferenceExpression)) {
                        if (left.containsAll(variablesInArgument0)) {
                            leftRowExpression.add(argument0);
                        }
                        else if (right.containsAll(variablesInArgument0)) {
                            rightRowExpression.add(argument0);
                        }
                    }

                    List<VariableReferenceExpression> variablesInArgument1 = extractAll(argument1);
                    if (!variablesInArgument1.isEmpty() && determinismEvaluator.isDeterministic(argument1) && !(argument1 instanceof VariableReferenceExpression)) {
                        if (left.containsAll(variablesInArgument1)) {
                            leftRowExpression.add(argument1);
                        }
                        else if (right.containsAll(variablesInArgument1)) {
                            rightRowExpression.add(argument1);
                        }
                    }
                }
            }
        }
        return ImmutableList.of(leftRowExpression, rightRowExpression);
    }
}
