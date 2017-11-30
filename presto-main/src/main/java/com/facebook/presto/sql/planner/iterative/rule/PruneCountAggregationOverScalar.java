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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * A count over a subquery can be reduced to a VALUES(1) provided
 * the subquery is a scalar
 */
public class PruneCountAggregationOverScalar
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        if (!parent.hasDefaultOutput() || parent.getOutputSymbols().size() != 1) {
            return Result.empty();
        }
        Map<Symbol, AggregationNode.Aggregation> assignments = parent.getAggregations();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : assignments.entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            requireNonNull(aggregation, "aggregation is null");
            Signature signature = aggregation.getSignature();
            FunctionCall functionCall = aggregation.getCall();
            if (!"count".equals(signature.getName()) || !functionCall.getArguments().isEmpty()) {
                return Result.empty();
            }
        }
        if (!assignments.isEmpty() && isScalar(parent.getSource(), context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(parent.getId(), parent.getOutputSymbols(), ImmutableList.of(ImmutableList.of(new LongLiteral("1")))));
        }
        return Result.empty();
    }
}
