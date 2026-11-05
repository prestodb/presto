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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isEvaluateFilterOnValuesEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates a deterministic filter against each row of a constant ValuesNode and keeps
 * only the rows for which the predicate is TRUE:
 * <pre>
 * - Filter (predicate)
 *     - Values (rows)
 * </pre>
 * into
 * <pre>
 * - Values (rows where predicate evaluates to TRUE)
 * </pre>
 * Rows whose predicate evaluates to FALSE or NULL are removed. If the predicate does not
 * evaluate to a constant for every row, or its evaluation fails for any row, the rule does
 * not fire. Removing all rows yields
 * an empty ValuesNode, which lets SimplifyPlanWithEmptyInput simplify the joins,
 * aggregations and unions above it.
 */
public class EvaluateFilterOnValues
        implements Rule<FilterNode>
{
    private static final Capture<ValuesNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(values().capturedAs(CHILD)));

    private final DeterminismEvaluator determinismEvaluator;
    private final ExpressionOptimizerManager expressionOptimizerManager;

    public EvaluateFilterOnValues(FunctionAndTypeManager functionAndTypeManager, ExpressionOptimizerManager expressionOptimizerManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEvaluateFilterOnValuesEnabled(session);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        ValuesNode source = captures.get(CHILD);
        RowExpression predicate = filterNode.getPredicate();
        List<VariableReferenceExpression> valuesOutputVariables = source.getOutputVariables();

        if (!determinismEvaluator.isDeterministic(predicate)) {
            return Result.empty();
        }
        if (!source.getRows().stream().allMatch(row -> row.stream().allMatch(determinismEvaluator::isDeterministic))) {
            return Result.empty();
        }

        ConnectorSession connectorSession = context.getSession().toConnectorSession();
        ExpressionOptimizer expressionOptimizer = expressionOptimizerManager.getExpressionOptimizer(connectorSession);
        ImmutableList.Builder<List<RowExpression>> retainedRows = ImmutableList.builder();
        for (List<RowExpression> row : source.getRows()) {
            verify(row.size() == valuesOutputVariables.size(), "Output variable does not match its value in ValuesNode");
            Map<VariableReferenceExpression, RowExpression> mapping = Streams.zip(
                            valuesOutputVariables.stream(),
                            row.stream(),
                            SimpleImmutableEntry::new)
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            RowExpression evaluated;
            try {
                evaluated = expressionOptimizer.optimize(inlineVariables(mapping, predicate), OPTIMIZED, connectorSession);
            }
            catch (RuntimeException e) {
                // Evaluation failed for this row (e.g. division by zero). Keep the filter so the
                // failure is raised at execution time, as it would be without this rule.
                return Result.empty();
            }
            if (!(evaluated instanceof ConstantExpression)) {
                return Result.empty();
            }
            if (Boolean.TRUE.equals(((ConstantExpression) evaluated).getValue())) {
                retainedRows.add(row);
            }
        }

        return Result.ofPlanNode(new ValuesNode(
                source.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                valuesOutputVariables,
                retainedRows.build(),
                source.getValuesNodeLabel()));
    }
}
