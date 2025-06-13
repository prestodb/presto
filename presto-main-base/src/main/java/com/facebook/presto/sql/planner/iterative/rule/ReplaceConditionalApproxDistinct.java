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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.SystemSessionProperties.isOptimizeConditionalApproxDistinctEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

/**
 * elimination of approx distinct on conditional constant values.
 * <p>
 * depending on the inner conditional, the expression is converted
 * to its equivalent arbitrary() expression.
 *     - approx_distinct(if(..., non-null)) -> arbitrary(if(..., 1, 0))
 *     - approx_distinct(if(..., non-null, non-null)) -> arbitrary(1)
 *     - approx_distinct(if(..., null, non-null)) -> arbitrary(if(..., 0, 1))
 *     - approx_distinct(if(..., null, null)) -> arbitrary(0)
 */

public class ReplaceConditionalApproxDistinct
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> SOURCE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(SOURCE)));

    private final StandardFunctionResolution functionResolution;

    public ReplaceConditionalApproxDistinct(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeConditionalApproxDistinctEnabled(session) &&
                functionResolution.supportsArbitraryFunction();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        VariableAllocator variableAllocator = context.getVariableAllocator();
        ProjectNode project = captures.get(SOURCE);
        Assignments inputs = project.getAssignments();
        boolean changed = false;
        Assignments.Builder assignments = Assignments.builder();

        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());
        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();
            RowExpression expr;

            if (!isApproxDistinct(aggregation)) {
                continue;
            }

            if (hasIfThenConstantInput(aggregation, inputs)) {
                SpecialFormExpression ifcondition = (SpecialFormExpression) inputs.get(
                        (VariableReferenceExpression) aggregation.getArguments().get(0));
                expr = variableAllocator.newVariable("expr", BIGINT);
                assignments.put((VariableReferenceExpression) expr, replaceIfExpression(ifcondition));
            }
            else {
                continue;
            }

            changed = true;
            aggregations.put(variable, new AggregationNode.Aggregation(
                    new CallExpression(
                            aggregation.getCall().getSourceLocation(),
                            "arbitrary",
                            functionResolution.arbitraryFunction(BIGINT),
                            BIGINT,
                            ImmutableList.of(expr)),
                    aggregation.getFilter(),
                    aggregation.getOrderBy(),
                    aggregation.isDistinct(),
                    aggregation.getMask()));
        }

        if (!changed) {
            return Result.empty();
        }

        ProjectNode modifiedProject = new ProjectNode(
                project.getSourceLocation(),
                project.getId(),
                project.getSource(),
                assignments.putAll(inputs).build(),
                project.getLocality());

        return Result.ofPlanNode(new AggregationNode(
                parent.getSourceLocation(),
                parent.getId(),
                modifiedProject,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashVariable(),
                parent.getGroupIdVariable(),
                parent.getAggregationId()));
    }

    private boolean isApproxDistinct(AggregationNode.Aggregation aggregation)
    {
        return functionResolution.isApproximateCountDistinctFunction(aggregation.getFunctionHandle());
    }

    private long convertConstant(ConstantExpression expr)
    {
        return isNull(expr) ? 0L : 1L;
    }

    private RowExpression replaceIfExpression(SpecialFormExpression ifcondition)
    {
        ConstantExpression truethen = (ConstantExpression) ifcondition.getArguments().get(1);
        ConstantExpression falsethen = (ConstantExpression) ifcondition.getArguments().get(2);
        RowExpression replace;

        if (isNull(truethen) ^ isNull(falsethen)) {
            // if(..., null, non-null) or if(..., non-null, null)
            replace = new SpecialFormExpression(
                    ifcondition.getSourceLocation(),
                    IF,
                    BIGINT,
                    ImmutableList.of(
                        ifcondition.getArguments().get(0),
                        constant(convertConstant(truethen), BIGINT),
                        constant(convertConstant(falsethen), BIGINT)));
        }
        else {
            // if(..., null, null) or if(..., non-null, non-null)
            replace = constant(convertConstant(truethen), BIGINT);
        }
        return replace;
    }

    private boolean hasIfThenConstantInput(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        RowExpression argument = aggregation.getArguments().get(0);
        RowExpression ifcondition = null;
        RowExpression truethen = null;
        RowExpression falsethen = null;

        if (argument instanceof VariableReferenceExpression) {
            ifcondition = inputs.get((VariableReferenceExpression) argument);
        }

        if (ifcondition instanceof SpecialFormExpression && ((SpecialFormExpression) ifcondition).getForm() == IF) {
            truethen = ((SpecialFormExpression) ifcondition).getArguments().get(1);
            falsethen = ((SpecialFormExpression) ifcondition).getArguments().get(2);
        }

        return truethen instanceof ConstantExpression && falsethen instanceof ConstantExpression;
    }
}
