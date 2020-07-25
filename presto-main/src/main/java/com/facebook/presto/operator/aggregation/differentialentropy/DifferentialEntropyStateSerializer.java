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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;

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
        int requiredBytes = DifferentialEntropyStateStrategy.getRequiredBytesForSerialization(strategy);
        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();
        DifferentialEntropyStateStrategy.serialize(strategy, sliceOut);
        VARBINARY.writeSlice(output, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            DifferentialEntropyState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.deserialize(input);
        if (strategy != null) {
            state.setStrategy(strategy);
        }
    }
}
