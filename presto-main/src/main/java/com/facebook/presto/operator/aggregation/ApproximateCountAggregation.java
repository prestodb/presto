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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.countError;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

@AggregationFunction(value = "count", approximate = true)
public final class ApproximateCountAggregation
{
    private static final int OUTPUT_VARCHAR_SIZE = 57;

    private ApproximateCountAggregation() {}

    @InputFunction
    public static void input(ApproximateCountState state, @SampleWeight long sampleWeight)
    {
        if (sampleWeight > 0) {
            state.setSamples(state.getSamples() + 1);
            state.setCount(state.getCount() + sampleWeight);
        }
    }

    @CombineFunction
    public static void combine(ApproximateCountState state, ApproximateCountState otherState)
    {
        state.setCount(state.getCount() + otherState.getCount());
        state.setSamples(state.getSamples() + otherState.getSamples());
    }

    @OutputFunction("varchar(57)")
    public static void output(ApproximateCountState state, double confidence, BlockBuilder out)
    {
        Slice value = Slices.utf8Slice(formatApproximateResult(state.getCount(), countError(state.getSamples(), state.getCount()), confidence, true));
        createVarcharType(OUTPUT_VARCHAR_SIZE).writeSlice(out, value);
    }

    public interface ApproximateCountState
            extends AccumulatorState
    {
        long getCount();

        void setCount(long value);

        long getSamples();

        void setSamples(long value);
    }
}
