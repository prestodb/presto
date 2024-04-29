package com.facebook.presto.operator.aggregation.noisyaggregation;

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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;

public class SfmSketchStateSerializer
        implements AccumulatorStateSerializer<SfmSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(SfmSketchState state, BlockBuilder out)
    {
        if (state.getSketch() == null) {
            out.appendNull();
        }
        else {
            DynamicSliceOutput output = new DynamicSliceOutput(state.getSketch().estimatedSerializedSize() + SizeOf.SIZE_OF_DOUBLE);
            output.appendDouble(state.getEpsilon());
            state.getSketch().serialize(output);
            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, SfmSketchState state)
    {
        Slice stateSlice = VARBINARY.getSlice(block, index);
        BasicSliceInput input = stateSlice.getInput();
        state.setEpsilon(input.readDouble());
        Slice pcsaSlice = input.slice();
        state.setSketch(SfmSketch.deserialize(pcsaSlice));
    }
}
