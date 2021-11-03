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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    private final StandardFunctionResolution functionResolution;

    public SimplifyCountOverConstant(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        boolean changed = false;
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());

        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments())) {
                changed = true;
                aggregations.put(variable, new AggregationNode.Aggregation(
                        new CallExpression(
                                "count",
                                functionResolution.countFunction(),
                                BIGINT,
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(new AggregationNode(
                parent.getId(),
                child,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashVariable(),
                parent.getGroupIdVariable()));
    }

    private boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        if (!functionResolution.isCountFunction(aggregation.getFunctionHandle()) || aggregation.getArguments().size() != 1) {
            return false;
        }

        RowExpression argument = aggregation.getArguments().get(0);
        RowExpression assigned = null;
        if (argument instanceof VariableReferenceExpression) {
            assigned = inputs.get((VariableReferenceExpression) argument);
        }

        return assigned instanceof ConstantExpression && !isNull(assigned);
    }
}
