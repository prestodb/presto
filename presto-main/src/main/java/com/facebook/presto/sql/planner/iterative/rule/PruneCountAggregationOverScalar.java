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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static java.util.Objects.requireNonNull;

/**
 * A count over a subquery can be reduced to a VALUES(1) provided
 * the subquery is a scalar
 */
public class PruneCountAggregationOverScalar
        implements Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(AggregationNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        AggregationNode parent = (AggregationNode) node;
        Map<Symbol, AggregationNode.Aggregation> assignments = parent.getAggregations();
        if (parent.hasDefaultOutput() && assignments.size() != 1) {
            return Optional.empty();
        }
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : assignments.entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            requireNonNull(aggregation, "aggregation is null");
            Signature signature = aggregation.getSignature();
            FunctionCall functionCall = aggregation.getCall();
            if (!"count".equals(signature.getName()) || !functionCall.getArguments().isEmpty()) {
                return Optional.empty();
            }
        }
        if (!assignments.isEmpty() && isScalar(parent.getSource(), context.getLookup())) {
            return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of(ImmutableList.of(new LongLiteral("1")))));
        }
        return Optional.empty();
    }
}
