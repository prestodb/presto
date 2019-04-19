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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StatisticAggregations
{
    private final Map<VariableReferenceExpression, Aggregation> aggregations;
    private final List<VariableReferenceExpression> groupingVariables;

    @JsonCreator
    public StatisticAggregations(
            @JsonProperty("aggregations") Map<VariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingVariables") List<VariableReferenceExpression> groupingVariables)
    {
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.groupingVariables = ImmutableList.copyOf(requireNonNull(groupingVariables, "groupingVariables is null"));
    }

    @JsonProperty
    public Map<VariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getGroupingVariables()
    {
        return groupingVariables;
    }

    public List<Symbol> getGroupingSymbols()
    {
        return groupingVariables.stream()
                .map(VariableReferenceExpression::getName)
                .map(Symbol::new)
                .collect(toImmutableList());
    }

    public Parts createPartialAggregations(SymbolAllocator symbolAllocator, FunctionManager functionManager)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> partialAggregation = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> finalAggregation = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mappings = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            InternalAggregationFunction function = functionManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression partialVariable = symbolAllocator.newVariable(functionManager.getFunctionMetadata(functionHandle).getName(), function.getIntermediateType());
            mappings.put(entry.getKey(), partialVariable);
            partialAggregation.put(partialVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getDisplayName(),
                            functionHandle,
                            function.getIntermediateType(),
                            originalAggregation.getArguments()),
                    originalAggregation.getFilter(),
                    originalAggregation.getOrderBy(),
                    originalAggregation.isDistinct(),
                    originalAggregation.getMask()));
            finalAggregation.put(entry.getKey(),
                    new Aggregation(
                            new CallExpression(
                                    originalAggregation.getCall().getDisplayName(),
                                    functionHandle,
                                    function.getFinalType(),
                                    ImmutableList.of(castToRowExpression(asSymbolReference(partialVariable)))),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
        }
        groupingVariables.forEach(symbol -> mappings.put(symbol, symbol));
        return new Parts(
                new StatisticAggregations(partialAggregation.build(), groupingVariables),
                new StatisticAggregations(finalAggregation.build(), groupingVariables),
                mappings.build());
    }

    public static class Parts
    {
        private final StatisticAggregations partialAggregation;
        private final StatisticAggregations finalAggregation;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mappings;

        public Parts(StatisticAggregations partialAggregation, StatisticAggregations finalAggregation, Map<VariableReferenceExpression, VariableReferenceExpression> mappings)
        {
            this.partialAggregation = requireNonNull(partialAggregation, "partialAggregation is null");
            this.finalAggregation = requireNonNull(finalAggregation, "finalAggregation is null");
            this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
        }

        public StatisticAggregations getPartialAggregation()
        {
            return partialAggregation;
        }

        public StatisticAggregations getFinalAggregation()
        {
            return finalAggregation;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getMappings()
        {
            return mappings;
        }
    }
}
