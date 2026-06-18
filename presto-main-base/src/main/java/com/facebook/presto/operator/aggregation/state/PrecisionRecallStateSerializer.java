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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleHistogram;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;

public class PrecisionRecallStateSerializer
        implements AccumulatorStateSerializer<PrecisionRecallState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(PrecisionRecallState state, BlockBuilder out)
    {
        int requiredBytes = SizeOf.SIZE_OF_BYTE; // has histograms;
        if (state.getTrueWeights() != null) {
            requiredBytes += state.getTrueWeights().getRequiredBytesForSerialization();
            requiredBytes += state.getFalseWeights().getRequiredBytesForSerialization();
        }

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();
        sliceOut.appendByte(state.getTrueWeights() == null ? 0 : 1);
        if (state.getTrueWeights() != null) {
            state.getTrueWeights().serialize(sliceOut);
            state.getFalseWeights().serialize(sliceOut);
        }

        VARBINARY.writeSlice(out, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            PrecisionRecallState state)
    {
        final SliceInput input = VARBINARY.getSlice(block, index).getInput();

        final byte hasHistograms = input.readByte();
        checkArgument(
                hasHistograms == 0 || hasHistograms == 1,
                "hasHistogram %s should be boolean-convertible", hasHistograms);
        if (hasHistograms == 1) {
            state.setTrueWeights(FixedDoubleHistogram.deserialize(input));
            state.setFalseWeights(FixedDoubleHistogram.deserialize(input));
        }
    }
}
