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

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@AggregationFunction("merge_binary")
public final class MergeBinaryHyperLogLogAggregation
{
    private MergeBinaryHyperLogLogAggregation()
    {
    }

    @InputFunction
    @IntermediateInputFunction
    public static void merge(SliceState state, @SqlType(StandardTypes.VARBINARY) Slice value) throws Throwable
    {
        HyperLogLogPlus input = HyperLogLogPlus.Builder.build(value.getBytes());
        HyperLogLogPlus previous = null;
        if (state.getSlice() != null) {
            previous = HyperLogLogPlus.Builder.build(state.getSlice().getBytes());
        }

        if (previous == null) {
            previous = input;
        }
        else {
            previous.addAll(input);
        }
        state.setSlice(Slices.wrappedBuffer(previous.getBytes()));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(SliceState state, BlockBuilder out) throws Throwable
    {
        BIGINT.writeLong(out, HyperLogLogPlus.Builder.build(state.getSlice().getBytes()).cardinality());
    }
}
