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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class PruneOrderByInAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionRegistry functionRegistry;

    public PruneOrderByInAggregation(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        if (node.getAggregations().values().stream().allMatch(aggregation -> aggregation.getOrderBy().isEmpty())) {
            return Result.empty();
        }

        boolean anyRewritten = false;
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            if (aggregation.getOrderBy().isEmpty()) {
                aggregations.put(entry);
            }
            // getAggregateFunctionImplementation can be expensive, so check it last.
            else if (functionRegistry.getAggregateFunctionImplementation(aggregation.getSignature()).isOrderSensitive()) {
                aggregations.put(entry);
            }
            else {
                anyRewritten = true;
                aggregations.put(entry.getKey(), new Aggregation(aggregation.getCall(), aggregation.getSignature(), aggregation.getMask(), ImmutableList.of(), ImmutableList.of()));
            }
        }

        if (!anyRewritten) {
            return Result.empty();
        }
        return Result.ofPlanNode(new AggregationNode(node.getId(), node.getSource(), aggregations.build(), node.getGroupingSets(), node.getStep(), node.getHashSymbol(), node.getGroupIdSymbol()));
    }
}
