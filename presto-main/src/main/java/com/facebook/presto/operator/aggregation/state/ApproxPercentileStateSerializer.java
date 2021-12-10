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

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class ApproxPercentileStateSerializer
        implements AccumulatorStateSerializer<ApproxPercentileState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(ApproxPercentileState state, BlockBuilder out)
    {
        if (state.getDigests() == null) {
            out.appendNull();
        }
        else {
            QuantileDigest[] digests = state.getDigests();
            int capacity = SIZE_OF_DOUBLE + SIZE_OF_INT;
            Slice[] slices = new Slice[digests.length];
            for (int i = 0; i < digests.length; i++) {
                QuantileDigest digest = digests[i];
                Slice serialized = digest.serialize();
                slices[i] = serialized;
                capacity += SIZE_OF_INT + serialized.length();
            }

            SliceOutput output = Slices.allocate(capacity).getOutput();
            output.appendDouble(state.getPercentile());
            output.appendInt(digests.length);
            for (Slice slice : slices) {
                output.appendInt(slice.length());
                output.appendBytes(slice);
            }

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, ApproxPercentileState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read percentile
        state.setPercentile(input.readDouble());

        // read length
        QuantileDigest[] digests = new QuantileDigest[input.readInt()];

        // read digests
        for (int i = 0; i < digests.length; i++) {
            digests[i] = new QuantileDigest(input.readSlice(input.readInt()));
            state.addMemoryUsage(digests[i].estimatedInMemorySizeInBytes());
        }
        state.setDigests(digests);
    }
}
