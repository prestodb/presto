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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class StatisticAggregations
{
    // outputVariables indicates the order of aggregations in the output
    private final List<VariableReferenceExpression> outputVariables;
    private final Map<VariableReferenceExpression, Aggregation> aggregations;
    private final List<VariableReferenceExpression> groupingVariables;

    @JsonCreator
    public StatisticAggregations(
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("aggregations") Map<VariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingVariables") List<VariableReferenceExpression> groupingVariables)
    {
        this.outputVariables = Collections.unmodifiableList(new ArrayList<>(requireNonNull(outputVariables, "outputVariables is null")));
        this.aggregations = Collections.unmodifiableMap(new LinkedHashMap<>(requireNonNull(aggregations, "aggregations is null")));
        this.groupingVariables = Collections.unmodifiableList(new ArrayList<>(requireNonNull(groupingVariables, "groupingVariables is null")));
        checkArgument(outputVariables.size() == aggregations.size(), "outputVariables and aggregations' sizes are different");
    }

    public StatisticAggregations(
            Map<VariableReferenceExpression, Aggregation> aggregations,
            List<VariableReferenceExpression> groupingVariables)
    {
        this.aggregations = Collections.unmodifiableMap(new LinkedHashMap<>(requireNonNull(aggregations, "aggregations is null")));
        this.groupingVariables = Collections.unmodifiableList(new ArrayList<>(requireNonNull(groupingVariables, "groupingVariables is null")));
        this.outputVariables = Collections.unmodifiableList(new ArrayList<>(aggregations.keySet()));
    }

    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
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
