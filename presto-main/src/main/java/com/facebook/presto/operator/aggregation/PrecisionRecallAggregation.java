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
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;

import java.math.BigInteger;
import java.util.ArrayList;

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
        state.setNumBins(numBins.intValue());

        if (pred == null) {
            return;
        }

        if (pred < 0 || pred > 1) {
            state.setNull(true);
            return;
        }

        Double weight = 1.0;
        if (weight == 0.0) {
            return;
        }
        if (weight < 0.0) {
            state.setNull(true);
        }
        state.setWeight(state.getWeight() + weight);

        final ArrayList<Double> weights = initializeWeightsIfNeeded(
                numBins.intValue(),
                outcome ? state.getTrueWeights() : state.getFalseWeights());
        final int ind = (int) (pred / numBins.doubleValue());
        weights.set(ind, weights.get(ind) + weight);
        if (outcome) {
            state.setTrueWeights(weights);
        }
        else {
            state.setFalseWeights(weights);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState PrecisionRecallState state,
            @AggregationState PrecisionRecallState otherState)
    {
        state.setNull(state.getNull() || otherState.getNull());

        final int numBins = Math.max(state.getNumBins(), otherState.getNumBins());
        state.setNumBins(numBins);

        final ArrayList<Double> trueWeights = initializeWeightsIfNeeded(numBins, state.getTrueWeights());
        final ArrayList<Double> falseWeights = initializeWeightsIfNeeded(numBins, state.getFalseWeights());
        final ArrayList<Double> otherTrueWeights = initializeWeightsIfNeeded(numBins, otherState.getTrueWeights());
        final ArrayList<Double> otherFalseWeights = initializeWeightsIfNeeded(numBins, otherState.getFalseWeights());
        for (int i = 0; i < trueWeights.size(); ++i) {
            trueWeights.set(i, trueWeights.get(i) + otherTrueWeights.get(i));
            falseWeights.set(i, falseWeights.get(i) + otherFalseWeights.get(i));
        }
        state.setTrueWeights(trueWeights);
        state.setFalseWeights(falseWeights);
    }

    @OutputFunction("map(double,double)")
    public static void output(@AggregationState PrecisionRecallState state, BlockBuilder out)
    {
        if (state.getNull()) {
            out.appendNull();
            return;
        }

        final ArrayList<Double> trueWeights = initializeWeightsIfNeeded(state.getNumBins(), state.getTrueWeights());
        final ArrayList<Double> falseWeights = initializeWeightsIfNeeded(state.getNumBins(), state.getFalseWeights());

        final Double totalTrueWeight = trueWeights.stream()
                .mapToDouble(Double::doubleValue)
                .sum();
        Double runningWeight = 0.0;
        Double runningTrueWeight = 0.0;
        BlockBuilder entryBuilder = out.beginBlockEntry();
        for (int i = trueWeights.size() - 1; i >= 0; --i) {
            runningWeight += trueWeights.get(i) + falseWeights.get(i);
            runningTrueWeight += trueWeights.get(i);

            final Double recall = safeDiv(runningTrueWeight, totalTrueWeight);
            final Double precision = safeDiv(runningTrueWeight, runningWeight);

            DoubleType.DOUBLE.writeDouble(entryBuilder, recall);
            DoubleType.DOUBLE.writeDouble(entryBuilder, precision);
        }
        out.closeEntry();
    }

    private PrecisionRecallAggregation() {}

    private static ArrayList<Double> initializeWeightsIfNeeded(int numBins, ArrayList<Double> weights)
    {
        return weights.isEmpty() ? new ArrayList<Double>(numBins) : weights;
    }

    private static Double safeDiv(Double nom, Double denom)
    {
        return denom == 0.0 ? 1.0 : nom / denom;
    }
}
