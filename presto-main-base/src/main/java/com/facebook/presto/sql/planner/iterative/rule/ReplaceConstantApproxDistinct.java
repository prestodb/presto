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
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOptimizeConstantApproxDistinctEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

/**
 * elimination of approx distinct on constant values.
 * <p>
 * transform APPROX_DISTINCT(CONST) to ARBITRARY(1)
 */

public class ReplaceConstantApproxDistinct
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> SOURCE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(SOURCE)));

    private final StandardFunctionResolution functionResolution;

    public ReplaceConstantApproxDistinct(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeConstantApproxDistinctEnabled(session);
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
        VariableReferenceExpression one = null;

        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());
        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();
            RowExpression expr;

            if (!isApproxDistinct(aggregation)) {
                continue;
            }

            if (hasConstantInput(aggregation, inputs)) {
                expr = constant(1L, BIGINT);
            }
            else if (hasIfThenConstantInput(aggregation, inputs)) {
                SpecialFormExpression ifcondition = (SpecialFormExpression) inputs.get(
                        (VariableReferenceExpression) aggregation.getArguments().get(0));

                expr = variableAllocator.newVariable("expr", BIGINT);
                assignments.put((VariableReferenceExpression) expr, new SpecialFormExpression(
                        ifcondition.getSourceLocation(),
                        IF,
                        BOOLEAN,
                        ImmutableList.of(
                        ifcondition.getArguments().get(0),
                        constant(1L, BIGINT),
                        constant(0L, BIGINT))));
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
                    Optional.empty(),
                    Optional.empty(),
                    false,
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

    private boolean constantExpr(RowExpression expr)
    {
        return expr instanceof ConstantExpression && !isNull(expr);
    }

    private boolean isApproxDistinct(AggregationNode.Aggregation aggregation)
    {
        return functionResolution.isApproximateCountDistinctFunction(aggregation.getFunctionHandle());
    }

    private boolean hasConstantInput(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        RowExpression argument = aggregation.getArguments().get(0);
        RowExpression input = null;
        if (argument instanceof VariableReferenceExpression) {
            input = inputs.get((VariableReferenceExpression) argument);
        }
        return constantExpr(input);
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

        return constantExpr(truethen) && (constantExpr(falsethen) || isNull(falsethen));
    }
}
