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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isInlineProjectionsOnValues;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer looks for ProjectNode followed by a ValuesNode and get the ProjectNode Evaluated.
 * When this rule is used on iterative optimizer, the rule could apply iteratively.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * ProjectNode (outputVariables)
 *   - ValuesNode
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * ValuesNode (outputVariables)
 * </pre>
 */
public class InlineProjectionsOnValues
        implements Rule<ProjectNode>
{
    private static final Capture<ValuesNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(values().capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public InlineProjectionsOnValues(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isInlineProjectionsOnValues(session);
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        ValuesNode source = captures.get(CHILD);
        List<List<RowExpression>> rows = source.getRows();
        List<VariableReferenceExpression> valuesOutputVariables = source.getOutputVariables();
        List<VariableReferenceExpression> projectOutputVariables = projectNode.getOutputVariables();
        List<RowExpression> projectRowExpressions = projectNode.getAssignments()
                .getExpressions()
                .stream()
                .collect(toImmutableList());

        // exclude non-deterministic function
        DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        if (!projectRowExpressions.stream().allMatch(determinismEvaluator::isDeterministic)) {
            return Result.empty();
        }
        if (!rows.stream().allMatch(row -> row.stream()
                .allMatch(determinismEvaluator::isDeterministic))) {
            return Result.empty();
        }

        //rewrite ProjectNode assignment expressions
        ImmutableList.Builder<List<RowExpression>> rowExpressionsListBuilder = ImmutableList.builder();
        for (List<RowExpression> rowExpressions : rows) {
            verify(rowExpressions.size() == valuesOutputVariables.size(), "Output variable does not match its value in ValuesNode");
            Map<VariableReferenceExpression, RowExpression> mapping = Streams.zip(
                            valuesOutputVariables.stream(),
                            rowExpressions.stream(),
                            SimpleImmutableEntry::new)
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            List<RowExpression> rowExpressionsInProject = projectRowExpressions.stream()
                    .map(expression -> inlineVariables(mapping, expression))
                    .collect(toImmutableList());
            rowExpressionsListBuilder.add(rowExpressionsInProject);
        }

        ValuesNode updatedProject = new ValuesNode(
                source.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                projectOutputVariables,
                rowExpressionsListBuilder.build(),
                Optional.empty());

        return Result.ofPlanNode(updatedProject);
    }
}
