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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AggregationNodeUtils
{
    private AggregationNodeUtils() {}

    public static AggregationNode.Aggregation count(FunctionAndTypeManager functionAndTypeManager)
    {
        return new AggregationNode.Aggregation(
                new CallExpression("count",
                        new FunctionResolution(functionAndTypeManager).countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static Set<VariableReferenceExpression> extractAggregationUniqueVariables(AggregationNode.Aggregation aggregation, TypeProvider types)
    {
        // types will be no longer needed once everything is RowExpression.
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        aggregation.getArguments().forEach(argument -> builder.addAll(extractAll(argument, types)));
        aggregation.getFilter().ifPresent(filter -> builder.addAll(extractAll(filter, types)));
        aggregation.getOrderBy().ifPresent(orderingScheme -> builder.addAll(orderingScheme.getOrderByVariables()));
        return builder.build();
    }

    private static List<VariableReferenceExpression> extractAll(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return VariablesExtractor.extractAll(castToExpression(expression), types);
        }
        return VariablesExtractor.extractAll(expression)
                .stream()
                .collect(toImmutableList());
    }
}
