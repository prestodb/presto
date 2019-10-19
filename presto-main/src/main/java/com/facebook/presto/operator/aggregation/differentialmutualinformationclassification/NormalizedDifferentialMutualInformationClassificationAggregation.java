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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import java.util.Map;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;

@AggregationFunction("normalized_differential_mutual_information_classification")
@Description("Computes the normalized mutual information score for classification")
public final class NormalizedDifferentialMutualInformationClassificationAggregation
{
    private NormalizedDifferentialMutualInformationClassificationAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BIGINT) long outcome,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max)
    {
        DifferentialEntropyStateStrategy featureStrategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getFeatureStrategy(),
                size,
                sample,
                weight,
                method.toStringUtf8().toLowerCase(ENGLISH),
                min,
                max);
        featureStrategy.add(sample, weight);
        DiscreteEntropyStateStrategy outcomeStrategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getOutcomeStrategy(),
                featureStrategy,
                weight);
        int effectiveOutcome = Long.valueOf(outcome).hashCode();
        outcomeStrategy.add(effectiveOutcome, weight);
        DifferentialEntropyStateStrategy featureStrategyForOutcome = state.getFeatureStrategyForOutcome(effectiveOutcome, featureStrategy);
        featureStrategyForOutcome.add(sample, weight);
        state.setFeatureStrategy(featureStrategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max)
    {
        input(
                state,
                size,
                outcome ? 1 : 0,
                sample,
                weight,
                method,
                min,
                max);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BIGINT) long outcome,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DifferentialEntropyStateStrategy featureStrategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getFeatureStrategy(),
                size,
                sample,
                weight);
        featureStrategy.add(sample, weight);
        DiscreteEntropyStateStrategy outcomeStrategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getOutcomeStrategy(),
                featureStrategy,
                weight);
        int effectiveOutcome = Long.valueOf(outcome).hashCode();
        outcomeStrategy.add(effectiveOutcome);
        DifferentialEntropyStateStrategy featureStrategyForOutcome = state.getFeatureStrategyForOutcome(effectiveOutcome, featureStrategy);
        featureStrategyForOutcome.add(sample, weight);
        state.setFeatureStrategy(featureStrategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(
                state,
                size,
                outcome ? 1 : 0,
                sample,
                weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BIGINT) long outcome,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        DifferentialEntropyStateStrategy featureStrategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getFeatureStrategy(),
                size,
                sample);
        featureStrategy.add(sample);
        DiscreteEntropyStateStrategy outcomeStrategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getOutcomeStrategy(),
                featureStrategy);
        int effectiveOutcome = Long.valueOf(outcome).hashCode();
        outcomeStrategy.add(effectiveOutcome);
        DifferentialEntropyStateStrategy featureStrategyForOutcome =
                state.getFeatureStrategyForOutcome(effectiveOutcome, featureStrategy);
        featureStrategyForOutcome.add(sample);
        state.setFeatureStrategy(featureStrategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        input(
                state,
                size,
                outcome ? 1 : 0,
                sample);
    }

    @CombineFunction
    public static void combine(
            @AggregationState DifferentialMutualInformationClassificationState state,
            @AggregationState DifferentialMutualInformationClassificationState otherState)
    {
        DifferentialEntropyStateStrategy featureStrategy = state.getFeatureStrategy();
        DifferentialEntropyStateStrategy otherFeatureStrategy = otherState.getFeatureStrategy();
        if (featureStrategy == null && otherFeatureStrategy != null) {
            state.setFeatureStrategy(otherFeatureStrategy.clone());
            state.setFeatureStrategiesForOutcomes(otherState.getFeatureStrategiesForOutcomes());
            return;
        }
        if (otherFeatureStrategy == null) {
            return;
        }
        DifferentialEntropyStateStrategy.combine(featureStrategy, otherFeatureStrategy);
        for (Map.Entry<Integer, DifferentialEntropyStateStrategy> entry : otherState.getFeatureStrategiesForOutcomes().entrySet()) {
            DifferentialEntropyStateStrategy currentOutcomeStateStrategy = state.getFeatureStrategyForOutcome(entry.getKey(), featureStrategy);
            DifferentialEntropyStateStrategy.combine(currentOutcomeStateStrategy, entry.getValue());
        }
    }

    @OutputFunction("double")
    public static void output(@AggregationState DifferentialMutualInformationClassificationState state, BlockBuilder out)
    {
        DiscreteEntropyStateStrategy outcomeStrategy = state.getOutcomeStrategy();
        if (outcomeStrategy == null) {
            DOUBLE.writeDouble(out, Double.NaN);
            return;
        }
        double outcomeEntropy = outcomeStrategy.calculateEntropy();
        if (outcomeEntropy == 0.0) {
            DOUBLE.writeDouble(out, Double.NaN);
            return;
        }
        DifferentialEntropyStateStrategy featureStrategy = state.getFeatureStrategy();
        Map<Integer, DifferentialEntropyStateStrategy> outcomeStrategies = state.getFeatureStrategiesForOutcomes();
        double totalPopulationWeight = outcomeStrategies.values().stream()
                .mapToDouble(DifferentialEntropyStateStrategy::getTotalPopulationWeight)
                .sum();
        if (totalPopulationWeight == 0.0) {
            DOUBLE.writeDouble(out, Double.NaN);
            return;
        }
        double featureEntropy = featureStrategy.calculateEntropy();
        if (featureEntropy == 0.0) {
            DOUBLE.writeDouble(out, Double.NaN);
            return;
        }
        double mutualInformation = featureEntropy;
        mutualInformation -= outcomeStrategies.values().stream()
                .mapToDouble(featureStrategyForOutcome -> {
                    double featureEntropyPerOutcome = featureStrategyForOutcome.calculateEntropy();
                    return featureStrategyForOutcome.getTotalPopulationWeight() / totalPopulationWeight * featureEntropyPerOutcome;
                })
                .sum();
        double normalizedMutualInformation = mutualInformation / outcomeEntropy;
        DOUBLE.writeDouble(out, Math.min(1.0, Math.max(0.0, normalizedMutualInformation)));
    }
}
