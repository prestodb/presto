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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

public class AggregationNodeUtils
{
    private AggregationNodeUtils() {}

    public static AggregationNode.Aggregation count(FunctionManager functionManager)
    {
        return new AggregationNode.Aggregation(
                new FunctionResolution(functionManager).countFunction(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static Set<Symbol> extractUnique(AggregationNode.Aggregation aggregation)
    {
        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
        aggregation.getArguments().forEach(argument -> builder.addAll(SymbolsExtractor.extractAll(argument)));
        aggregation.getFilter().ifPresent(filter -> builder.addAll(SymbolsExtractor.extractAll(filter)));
        aggregation.getOrderBy().ifPresent(orderingScheme -> builder.addAll(orderingScheme.getOrderBy()));
        return builder.build();
    }
}
