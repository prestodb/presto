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

import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeVarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateVarianceState;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.sumError;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

@AggregationFunction(value = "sum", approximate = true)
public final class ApproximateSumAggregations
{
    private static final int OUTPUT_VARCHAR_SIZE = 57;

    private ApproximateSumAggregations() {}

    @InputFunction
    public static void input(ApproximateDoubleSumState state, @SqlType(StandardTypes.DOUBLE) double value, @SampleWeight long sampleWeight)
    {
        state.setWeightedCount(state.getWeightedCount() + sampleWeight);
        state.setSum(state.getSum() + value * sampleWeight);
        updateVarianceState(state, value);
    }

    @CombineFunction
    public static void combine(ApproximateDoubleSumState state, ApproximateDoubleSumState otherState)
    {
        state.setSum(state.getSum() + otherState.getSum());
        state.setWeightedCount(state.getWeightedCount() + otherState.getWeightedCount());
        mergeVarianceState(state, otherState);
    }

    @OutputFunction("varchar(57)")
    public static void output(ApproximateDoubleSumState state, double confidence, BlockBuilder out)
    {
        if (state.getWeightedCount() == 0) {
            out.appendNull();
            return;
        }

        String result = formatApproximateResult(
                state.getSum(),
                sumError(state.getCount(), state.getWeightedCount(), state.getM2(), state.getMean()),
                confidence,
                false);
        createVarcharType(OUTPUT_VARCHAR_SIZE).writeSlice(out, Slices.utf8Slice(result));
    }

    @InputFunction
    public static void input(ApproximateLongSumState state, @SqlType(StandardTypes.BIGINT) long value, @SampleWeight long sampleWeight)
    {
        state.setWeightedCount(state.getWeightedCount() + sampleWeight);
        state.setSum(state.getSum() + value * sampleWeight);
        updateVarianceState(state, value);
    }

    @CombineFunction
    public static void combine(ApproximateLongSumState state, ApproximateLongSumState otherState)
    {
        state.setSum(state.getSum() + otherState.getSum());
        state.setWeightedCount(state.getWeightedCount() + otherState.getWeightedCount());
        mergeVarianceState(state, otherState);
    }

    @OutputFunction("varchar(57)")
    public static void evaluateFinal(ApproximateLongSumState state, double confidence, BlockBuilder out)
    {
        if (state.getWeightedCount() == 0) {
            out.appendNull();
            return;
        }

        String result = formatApproximateResult(
                state.getSum(),
                sumError(state.getCount(), state.getWeightedCount(), state.getM2(), state.getMean()),
                confidence,
                true);
        createVarcharType(OUTPUT_VARCHAR_SIZE).writeSlice(out, Slices.utf8Slice(result));
    }

    public interface ApproximateDoubleSumState
            extends VarianceState
    {
        double getSum();

        void setSum(double value);

        long getWeightedCount();

        void setWeightedCount(long value);
    }

    public interface ApproximateLongSumState
            extends VarianceState
    {
        long getSum();

        void setSum(long value);

        long getWeightedCount();

        void setWeightedCount(long value);
    }
}
