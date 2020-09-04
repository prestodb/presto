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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.ApproxResultsOption;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class CountDistinctToApproxDistinct
        implements Rule<AggregationNode>
{
    private final Metadata metadata;

    public CountDistinctToApproxDistinct(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return aggregation().matching(this::isCountDistinct);
    }

    private boolean isCountDistinct(AggregationNode node)
    {
        return node.getAggregations().values().stream().anyMatch(a -> a.isDistinct() && a.getCall().getDisplayName().equals("count"));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        ApproxResultsOption approxResultsOption = SystemSessionProperties.getApproxResultsOption(session);
        return isApproxDistinct(approxResultsOption);
    }

    public static boolean isApproxDistinct(ApproxResultsOption option)
    {
        return option == ApproxResultsOption.APPROX_DISTINCT;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (VariableReferenceExpression var : node.getAggregations().keySet()) {
            AggregationNode.Aggregation aggregation = node.getAggregations().get(var);
            if (aggregation.isDistinct() && aggregation.getCall().getDisplayName().equals("count")) {
                CallExpression call = aggregation.getCall();
                AggregationNode.Aggregation aggregation1 = new AggregationNode.Aggregation(
                        new CallExpression(
                                "approx_distinct",
                                metadata.getFunctionManager().lookupFunction("approx_distinct", fromTypes(call.getType())),
                                call.getType(),
                                call.getArguments()),
                        aggregation.getFilter(),
                        aggregation.getOrderBy(),
                        false,
                        aggregation.getMask());
                aggregations.put(var, aggregation1);
            }
            else {
                aggregations.put(var, aggregation);
            }
        }

        AggregationNode newNode = new AggregationNode(
                node.getId(),
                node.getSource(),
                aggregations.build(),
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable());
        return Result.ofPlanNode(newNode);
    }
}
