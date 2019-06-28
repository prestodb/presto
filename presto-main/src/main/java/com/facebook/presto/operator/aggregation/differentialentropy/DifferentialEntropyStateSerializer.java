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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class DifferentialEntropyStateSerializer
        implements AccumulatorStateSerializer<DifferentialEntropyState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(DifferentialEntropyState state, BlockBuilder output)
    {
        DifferentialEntropyStateStrategy strategy = state.getStrategy();

        if (strategy == null) {
            SliceOutput sliceOut = Slices.allocate(SizeOf.SIZE_OF_INT).getOutput();
            sliceOut.appendInt(0);
            VARBINARY.writeSlice(output, sliceOut.getUnderlyingSlice());
            return;
        }

        int requiredBytes = SizeOf.SIZE_OF_INT + // Method
                strategy.getRequiredBytesForSerialization(); // stateStrategy;

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();

        if (strategy instanceof UnweightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof WeightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof FixedHistogramMleStateStrategy) {
            sliceOut.appendInt(3);
        }
        else if (strategy instanceof FixedHistogramJacknifeStateStrategy) {
            sliceOut.appendInt(4);
        }
        else {
            verify(false, format("Strategy cannot be serialized: %s", strategy.getClass().getSimpleName()));
        }

        strategy.serialize(sliceOut);
        VARBINARY.writeSlice(output, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            DifferentialEntropyState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        int method = input.readInt();
        switch (method) {
            case 0:
                verify(state.getStrategy() == null, "strategy is not null for null method");
                return;
            case 1:
                state.setStrategy(UnweightedReservoirSampleStateStrategy.deserialize(input));
                return;
            case 2:
                state.setStrategy(WeightedReservoirSampleStateStrategy.deserialize(input));
                return;
            case 3:
                state.setStrategy(FixedHistogramMleStateStrategy.deserialize(input));
                return;
            case 4:
                state.setStrategy(FixedHistogramJacknifeStateStrategy.deserialize(input));
                return;
            default:
                verify(false, format("Unknown method code when deserializing: %s", method));
        }
    }
}
