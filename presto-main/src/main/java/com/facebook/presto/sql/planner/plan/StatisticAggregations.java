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
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public Parts createPartialAggregations(PlanVariableAllocator variableAllocator, FunctionManager functionManager)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> partialAggregation = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> finalAggregation = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            InternalAggregationFunction function = functionManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression partialVariable = variableAllocator.newVariable(functionManager.getFunctionMetadata(functionHandle).getName(), function.getIntermediateType());
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
                                    ImmutableList.of(partialVariable)),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
        }
        return new Parts(
                new StatisticAggregations(partialAggregation.build(), groupingVariables),
                new StatisticAggregations(finalAggregation.build(), groupingVariables));
    }

    public static class Parts
    {
        private final StatisticAggregations partialAggregation;
        private final StatisticAggregations finalAggregation;

        public Parts(StatisticAggregations partialAggregation, StatisticAggregations finalAggregation)
        {
            this.partialAggregation = requireNonNull(partialAggregation, "partialAggregation is null");
            this.finalAggregation = requireNonNull(finalAggregation, "finalAggregation is null");
        }

        public StatisticAggregations getPartialAggregation()
        {
            return partialAggregation;
        }

        public StatisticAggregations getFinalAggregation()
        {
            return finalAggregation;
        }
    }
}
