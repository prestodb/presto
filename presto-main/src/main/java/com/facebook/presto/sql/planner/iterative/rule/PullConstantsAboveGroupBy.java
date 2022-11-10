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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isOptimizeConstantGroupingKeys;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.isConstant;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

/**
 * Transforms:
 * <pre>
 * - GroupBy key1, <const_expr>, key2
 * </pre>
 * Into:
 * <pre>
 * - Project <const_expr>
 *    - GroupBy key1, key2
 * </pre>
 */
public class PullConstantsAboveGroupBy
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> SOURCE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN =
            aggregation()
            .matching(agg -> agg.getGroupingSetCount() == 1)
            .with(source().matching(project().capturedAs(SOURCE)));

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeConstantGroupingKeys(session);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        if (!isEnabled(context.getSession())) {
            return Result.empty();
        }

        // for each variable references in grouping keys, check if the source expression defines them as constants
        ProjectNode source = captures.get(SOURCE);
        List<VariableReferenceExpression> outputVariables = parent.getOutputVariables();

        Map<VariableReferenceExpression, RowExpression> constSourceVars = extractConstVars(source, outputVariables);

        List<VariableReferenceExpression> groupingKeys = parent.getGroupingKeys();
        List<VariableReferenceExpression> newGroupingKeys =
                groupingKeys.stream()
                        .filter(key -> !constSourceVars.containsKey(key))
                        .collect(toImmutableList());

        if (constSourceVars.isEmpty() || newGroupingKeys.equals(groupingKeys)) {
            return Result.empty();
        }

        // Can't pull up constant grouping keys if there are no other
        // grouping keys because it will turn the aggregation into a global
        // aggregation, which has different semantics on an empty input.
        // A grouped aggregation with 0 rows of input will output 0 rows, but
        // a global aggregation will always return one row
        if (newGroupingKeys.isEmpty()) {
            return Result.empty();
        }

        AggregationNode newAgg = new AggregationNode(
                parent.getSourceLocation(),
                parent.getId(),
                source,
                parent.getAggregations(),
                singleGroupingSet(newGroupingKeys),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashVariable(),
                parent.getGroupIdVariable());

        Map<VariableReferenceExpression, RowExpression> remainingVars =
                outputVariables.stream()
                        .filter(var -> !constSourceVars.containsKey(var))
                        .collect(toImmutableMap(identity(), identity()));

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(constSourceVars);
        assignments.putAll(remainingVars);
        return Result.ofPlanNode(
                new ProjectNode(
                    parent.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newAgg,
                    assignments.build(),
                    source.getLocality()));
    }

    private static Map<VariableReferenceExpression, RowExpression> extractConstVars(ProjectNode projectNode, List<VariableReferenceExpression> outputVariables)
    {
        return projectNode.getAssignments().entrySet().stream()
                .filter((entry) -> isConstantRowExpr(entry.getValue()) && outputVariables.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isConstantRowExpr(RowExpression expr)
    {
        if (isExpression(expr)) {
            return isConstant(castToExpression(expr));
        }
        return expr instanceof ConstantExpression;
    }
}
