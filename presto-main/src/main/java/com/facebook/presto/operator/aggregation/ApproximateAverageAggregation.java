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

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class ApproximateAverageAggregation
        extends AbstractApproximateAggregationFunction<ApproximateAverageAggregation.ApproximateAverageState>
{
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

    private final boolean inputIsLong;

    public ApproximateAverageAggregation(Type parameterType)
    {
        // Intermediate type should be a fixed width structure
        super(VARCHAR, VARCHAR, parameterType);

        if (parameterType == BIGINT) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE, but was " + parameterType);
        }
    }

    @Override
    protected void processInput(ApproximateAverageState state, Block block, int index, long sampleWeight)
    {
        double inputValue;
        if (inputIsLong) {
            inputValue = block.getLong(index);
        }
        else {
            inputValue = block.getDouble(index);
        }

        long currentCount = state.getCount();
        double currentMean = state.getMean();

        // Use numerically stable variant
        for (int i = 0; i < sampleWeight; i++) {
            currentCount++;
            double delta = inputValue - currentMean;
            currentMean += (delta / currentCount);
            // update m2 inline
            state.setM2(state.getM2() + (delta * (inputValue - currentMean)));
        }

        // write values back out
        state.setCount(currentCount);
        state.setMean(currentMean);
        state.setSamples(state.getSamples() + 1);
    }

    @Override
    protected void combineState(ApproximateAverageState state, ApproximateAverageState otherState)
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

    @Override
    protected void evaluateFinal(ApproximateAverageState state, double confidence, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            String result = formatApproximateAverage(state.getSamples(), state.getMean(), state.getM2() / state.getCount(), confidence);
            out.appendSlice(Slices.utf8Slice(result));
        }
    }

    private static String formatApproximateAverage(long samples, double mean, double variance, double confidence)
    {
        return formatApproximateResult(mean, Math.sqrt(variance / samples), confidence, false);
    }
}
