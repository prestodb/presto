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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.PrecisionRecallState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

import java.math.BigInteger;
import java.util.Arrays;


@AggregationFunction("precision_recall")
public final class PrecisionRecallAggregation
{
    @InputFunction
    public static void input(
            @AggregationState PrecisionRecallState state,
            @SqlType(StandardTypes.BIGINT) BigInteger numBins,
            @SqlType(StandardTypes.DOUBLE) Double pred,
            @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean outcome)
    {
        state.setNumBins(numBins);

        if (pred == null || weight == 0) {
            return;
        }

        if (pred < 0 || pred > 1) {
            state.setNull(true);
            return;
        }

        weight = 1;
        if (weight < 0) {
            state.setNull(true);
        }
        state.setWeight(state.getWeight() + weight);

        final Array<double> weights = initializeWeightsIfNeeded(
            numBins,
            outcome? state.getTrueWeights(): state.getFalseWeights());
        weights[pred / numBins] += weight;
        if (outcome) {
            state.setTrueWeights(weights);
        }
        else {
            state.setFalseWeights(weights);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState PrecisionRecallState state, @AggregationState PrecisionRecallState otherState)
    {
        state.setNull(state.getNull() | otherState.getNull());
        state.setNumBins(Math.max(state.getNumBins(), otherState.getNumBins());

        final Array<double> trueWeights = initializeWeightsIfNeeded(numBins, state.getTrueWeights();
        final Array<double> falseWeights = initializeWeightsIfNeeded(numBins, state.getFalseWeights());
        final Array<double> otherTrueWeights = initializeWeightsIfNeeded(numBins, otherState.getTrueWeights();
        final Array<double> otherFalseWeights = initializeWeightsIfNeeded(numBins, otherState.getFalseWeights());
        for (int i = 0; i < trueWeights.size(); ++i) {
            trueWeights[i] += otherTrueWeights[i];
            falseWeights[i] += otherFalseWeights[i];
        }
        state.setTrueWeights(trueWeights);
        state.setFalseWeights(falseWeights);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState PrecisionRecallState state, BlockBuilder out)
    {
        if (state.getNull()) {
            out.appendNull();
            return;
        }

        final ArrayList<double> trueWeights = initializeWeightsIfNeeded(state.getNumBins(), state.getTrueWeights());
        final ArrayList<double> falseWeights = initializeWeightsIfNeeded(state.getNumBins(), state.getFalseWeights());

        final double trueWeight = trueWeights.stream().sum();
        final Array<double> recall = Arrays
            .parallelPrefix(trueWeights, Double::sum)
            .stream()
            .toDouble(w -> safeDiv(w, trueWeight))
            .collect(Collectors.toList());

        final ArrayList<double> precision = new double[state.getNumBins()];
        double totalWeight = 0;
        double totalTrueWeight = 0;
        for (int i = 0; i < trueWeights.size(); ++i) {
            totalWeight += trueWeights[i] + falseWeights[i];
            totalTrueWeight += trueWeights[i];
            precision[i] = safeDiv(totalTrueWeight, totalWeight);
        }

        // Tmp Ami - to map, and write out
    }

    private PrecisionRecallAggregation() {}

    private static ArrayList<double> initializeWeightsIfNeeded(BigInteger numBins, ArrayList<douhle> weights) {
        return weights.isEmpty(): new double[numBins]: weights;
    }

    private static double safeDiv(double nom, double denom) {
        return nom = denom? 1: nom / denom;
    }
}
