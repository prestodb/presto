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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;

public class RemoveInsensitiveAggregateDistinct
        implements Rule<AggregationNode>
{
    private static final Set<QualifiedObjectName> DISTINCT_INSENSITIVE_AGGREGATION_NAMES = ImmutableSet.<QualifiedObjectName>builder()
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "any_value"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "arbitrary"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "bitwise_and_agg"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "bitwise_or_agg"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "bool_and"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "bool_or"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "every"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "max"))
            .add(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "min"))
            .build();

    private final Pattern<AggregationNode> pattern = aggregation()
            .matching(this::hasInsensitiveDistinct);

    private final FunctionAndTypeManager functionAndTypeManager;

    public RemoveInsensitiveAggregateDistinct(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
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
            if (isDistinctInsensitiveAggregation(aggregation)) {
                aggregations.put(entry.getKey(),
                        new Aggregation(
                                aggregation.getCall(),
                                aggregation.getFilter(),
                                aggregation.getOrderBy(),
                                false,
                                aggregation.getMask()));
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

    private boolean hasInsensitiveDistinct(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .anyMatch(this::isDistinctInsensitiveAggregation);
    }

    private boolean isDistinctInsensitiveAggregation(Aggregation aggregation)
    {
        return aggregation.isDistinct()
                && DISTINCT_INSENSITIVE_AGGREGATION_NAMES.contains(functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName())
                && aggregation.getArguments().size() == 1;
    }
}
