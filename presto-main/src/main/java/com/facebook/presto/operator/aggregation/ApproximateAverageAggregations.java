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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

@AggregationFunction(value = "avg", approximate = true)
public final class ApproximateAverageAggregations
{
    private static final int OUTPUT_VARCHAR_SIZE = 57;

    private ApproximateAverageAggregations() {}

    @InputFunction
    public static void bigintInput(ApproximateAverageState state, @SqlType(StandardTypes.BIGINT) long value, @SampleWeight long sampleWeight)
    {
        doubleInput(state, (double) value, sampleWeight);
    }

    @InputFunction
    public static void doubleInput(ApproximateAverageState state, @SqlType(StandardTypes.DOUBLE) double value, @SampleWeight long sampleWeight)
    {
        long currentCount = state.getCount();
        double currentMean = state.getMean();

        // Use numerically stable variant
        for (int i = 0; i < sampleWeight; i++) {
            currentCount++;
            double delta = value - currentMean;
            currentMean += (delta / currentCount);
            // update m2 inline
            state.setM2(state.getM2() + (delta * (value - currentMean)));
        }

        // write values back out
        state.setCount(currentCount);
        state.setMean(currentMean);
        state.setSamples(state.getSamples() + 1);
    }

    @CombineFunction
    public static void combine(ApproximateAverageState state, ApproximateAverageState otherState)
    {
        long inputCount = otherState.getCount();
        long inputSamples = otherState.getSamples();
        double inputMean = otherState.getMean();
        double inputM2 = otherState.getM2();

        long currentCount = state.getCount();
        double currentMean = state.getMean();
        double currentM2 = state.getM2();

        // Use numerically stable variant
        if (inputCount > 0) {
            long newCount = currentCount + inputCount;
            double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
            double delta = inputMean - currentMean;
            double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

            state.setCount(newCount);
            state.setSamples(state.getSamples() + inputSamples);
            state.setMean(newMean);
            state.setM2(newM2);
        }
    }

    @OutputFunction("varchar(57)")
    public static void output(ApproximateAverageState state, double confidence, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            String result = formatApproximateAverage(state.getSamples(), state.getMean(), state.getM2() / state.getCount(), confidence);
            createVarcharType(OUTPUT_VARCHAR_SIZE).writeString(out, result);
        }
    }

    private static String formatApproximateAverage(long samples, double mean, double variance, double confidence)
    {
        return formatApproximateResult(mean, Math.sqrt(variance / samples), confidence, false);
    }

    public interface ApproximateAverageState
            extends AccumulatorState
    {
        long getCount();

        void setCount(long value);

        long getSamples();

        void setSamples(long value);

        double getMean();

        void setMean(double value);

        double getM2();

        void setM2(double value);
    }
}
