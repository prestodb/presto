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
import com.google.common.collect.ImmutableMap;

import java.util.Map.Entry;

import static com.facebook.presto.SystemSessionProperties.isOptimizeConditionalApproxDistinctEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * elimination of approx distinct on conditional constant values.
 * <p>
 * depending on the inner conditional, the expression is converted
 * to its equivalent arbitrary() expression.
 *
 *     - approx_distinct(if(..., non-null)) -> arbitrary(if(..., 1, NULL))
 *     - approx_distinct(if(..., null, non-null)) -> arbitrary(if(..., NULL, 1))
 *     - approx_distinct(if(..., null, null)) -> arbitrary(0)
 *
 * An intermediate projection is inserted to convert any NULL arbitrary output
 * to zero values.
 */
public class ReplaceConditionalApproxDistinct
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> SOURCE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(SOURCE)));

    private final StandardFunctionResolution functionResolution;

    private static final String ARBITRARY = "arbitrary";

    public ReplaceConditionalApproxDistinct(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeConditionalApproxDistinctEnabled(session);
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
        boolean changed = false;
        ProjectNode project = captures.get(SOURCE);
        Assignments.Builder outputs = Assignments.builder();
        Assignments.Builder inputs = Assignments.builder();

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();
            SpecialFormExpression replaced;
            VariableReferenceExpression intermediate;
            VariableReferenceExpression expression;

            if (!isApproxDistinct(aggregation) || !aggregationIsReplaceable(aggregation, project.getAssignments())) {
                aggregations.put(variable, aggregation);
                outputs.put(variable, variable);
                continue;
            }
            changed = true;
            replaced = (SpecialFormExpression) project.getAssignments().get(
                    (VariableReferenceExpression) aggregation.getArguments().get(0));

            expression = variableAllocator.newVariable("expression", BIGINT);
            inputs.put(expression, replaceIfExpression(replaced));

            intermediate = variableAllocator.newVariable("intermediate", BIGINT);
            aggregations.put(intermediate, new AggregationNode.Aggregation(
                    new CallExpression(
                            aggregation.getCall().getSourceLocation(),
                            ARBITRARY,
                            functionResolution.arbitraryFunction(BIGINT),
                            BIGINT,
                            ImmutableList.of(expression)),
                    aggregation.getFilter(),
                    aggregation.getOrderBy(),
                    aggregation.isDistinct(),
                    aggregation.getMask()));

            outputs.put(variable, new SpecialFormExpression(
                    COALESCE,
                    BIGINT,
                    ImmutableList.of(
                        intermediate,
                        constant(0L, BIGINT))));
        }

        if (!changed) {
            return Result.empty();
        }

        ProjectNode child = new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                project.getSource(),
                inputs.putAll(project.getAssignments()).build(),
                project.getLocality());

        AggregationNode aggregation = new AggregationNode(
                parent.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                child,
                aggregations.build(),
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashVariable(),
                parent.getGroupIdVariable(),
                parent.getAggregationId());

        aggregation.getHashVariable().ifPresent(hashvariable -> outputs.put(hashvariable, hashvariable));
        aggregation.getGroupingSets().getGroupingKeys().forEach(groupingKey -> outputs.put(groupingKey, groupingKey));
        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                aggregation,
                outputs.build()));
    }

    private boolean isApproxDistinct(AggregationNode.Aggregation aggregation)
    {
        return functionResolution.isApproximateCountDistinctFunction(aggregation.getFunctionHandle());
    }

    private ConstantExpression convertConstant(ConstantExpression expression)
    {
        return isNull(expression) ? constantNull(BIGINT) : constant(1L, BIGINT);
    }

    private RowExpression replaceIfExpression(SpecialFormExpression ifCondition)
    {
        ConstantExpression trueThen = (ConstantExpression) ifCondition.getArguments().get(1);
        ConstantExpression falseThen = (ConstantExpression) ifCondition.getArguments().get(2);
        RowExpression replace;

        if ((isNull(trueThen) && !isNull(falseThen)) || (!isNull(trueThen) && isNull(falseThen))) {
            // if(..., null, non-null) or if(..., non-null, null)
            replace = new SpecialFormExpression(
                    ifCondition.getSourceLocation(),
                    IF,
                    BIGINT,
                    ImmutableList.of(
                        ifCondition.getArguments().get(0),
                        convertConstant(trueThen),
                        convertConstant(falseThen)));
        }
        else {
            // if(..., null, null)
            checkState(isNull(trueThen) && isNull(falseThen),
                    "expected true (%s) and false (%s) predicates to be null",
                    trueThen, falseThen);
            replace = convertConstant(trueThen);
        }
        return replace;
    }

    private boolean aggregationIsReplaceable(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        RowExpression argument = aggregation.getArguments().get(0);
        RowExpression ifCondition = null;
        RowExpression trueThen = null;
        RowExpression falseThen = null;

        if (argument instanceof VariableReferenceExpression) {
            ifCondition = inputs.get((VariableReferenceExpression) argument);
        }

        if (ifCondition instanceof SpecialFormExpression && ((SpecialFormExpression) ifCondition).getForm() == IF) {
            trueThen = ((SpecialFormExpression) ifCondition).getArguments().get(1);
            falseThen = ((SpecialFormExpression) ifCondition).getArguments().get(2);
        }

        return trueThen instanceof ConstantExpression &&
                falseThen instanceof ConstantExpression &&
                (isNull(trueThen) || isNull(falseThen));
    }
}
