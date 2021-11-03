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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
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

    public Parts splitIntoPartialAndFinal(PlanVariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
    {
        return split(variableAllocator, functionAndTypeManager, false);
    }

    public Parts splitIntoPartialAndIntermediate(PlanVariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
    {
        return split(variableAllocator, functionAndTypeManager, true);
    }

    private Parts split(PlanVariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager, boolean intermediate)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> finalOrIntermediateAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> partialAggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            InternalAggregationFunction function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);

            // create partial aggregation
            VariableReferenceExpression partialVariable = variableAllocator.newVariable(functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName(), function.getIntermediateType());
            partialAggregations.put(partialVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getDisplayName(),
                            functionHandle,
                            function.getIntermediateType(),
                            originalAggregation.getArguments()),
                    originalAggregation.getFilter(),
                    originalAggregation.getOrderBy(),
                    originalAggregation.isDistinct(),
                    originalAggregation.getMask()));

            // create final aggregation
            finalOrIntermediateAggregations.put(entry.getKey(),
                    new Aggregation(
                            new CallExpression(
                                    originalAggregation.getCall().getDisplayName(),
                                    functionHandle,
                                    intermediate ? function.getIntermediateType() : function.getFinalType(),
                                    ImmutableList.of(partialVariable)),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
        }

        StatisticAggregations finalOrIntermediateAggregation = new StatisticAggregations(finalOrIntermediateAggregations.build(), groupingVariables);
        return new Parts(
                intermediate ? Optional.empty() : Optional.of(finalOrIntermediateAggregation),
                intermediate ? Optional.of(finalOrIntermediateAggregation) : Optional.empty(),
                new StatisticAggregations(partialAggregations.build(), groupingVariables));
    }

    public static class Parts
    {
        private final Optional<StatisticAggregations> finalAggregation;
        private final Optional<StatisticAggregations> intermediateAggregation;
        private final StatisticAggregations partialAggregation;

        public Parts(
                Optional<StatisticAggregations> finalAggregation,
                Optional<StatisticAggregations> intermediateAggregation,
                StatisticAggregations partialAggregation)
        {
            this.finalAggregation = requireNonNull(finalAggregation, "finalAggregation is null");
            this.intermediateAggregation = requireNonNull(intermediateAggregation, "intermediateAggregation is null");
            checkArgument(
                    finalAggregation.isPresent() ^ intermediateAggregation.isPresent(),
                    "only final or only intermediate aggregation is expected to be present");
            this.partialAggregation = requireNonNull(partialAggregation, "partialAggregation is null");
        }

        public StatisticAggregations getFinalAggregation()
        {
            return finalAggregation.orElseThrow(() -> new IllegalStateException("finalAggregation is not present"));
        }

        public StatisticAggregations getIntermediateAggregation()
        {
            return intermediateAggregation.orElseThrow(() -> new IllegalStateException("intermediateAggregation is not present"));
        }

        public StatisticAggregations getPartialAggregation()
        {
            return partialAggregation;
        }
    }
}
