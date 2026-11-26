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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.spi.plan.AggregationNode.Aggregation.removeDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class RemoveInsensitiveAggregateDistinct
        implements Rule<AggregationNode>
{
    private final Pattern<AggregationNode> pattern = aggregation()
            .matching(this::canRemoveDistinct);

    private final FunctionAndTypeManager functionAndTypeManager;

    public RemoveInsensitiveAggregateDistinct(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            if (canRemoveDistinct(aggregation)) {
                aggregations.put(entry.getKey(), removeDistinct(entry.getValue()));
            }
            else {
                aggregations.put(entry);
            }
        }
        return Result.ofPlanNode(
                new AggregationNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getSource(),
                        aggregations.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedVariables(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable(),
                        node.getAggregationId()));
    }

    private boolean canRemoveDistinct(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .anyMatch(this::canRemoveDistinct);
    }

    private boolean canRemoveDistinct(Aggregation aggregation)
    {
        AggregationFunctionImplementation implementation = functionAndTypeManager
                .getAggregateFunctionImplementation(aggregation.getFunctionHandle());
        return aggregation.isDistinct()
                && (!implementation.isDistinctSensitive())
                && (!implementation.isOrderSensitive());
    }
}
