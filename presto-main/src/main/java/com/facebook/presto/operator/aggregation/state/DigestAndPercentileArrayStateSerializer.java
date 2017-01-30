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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class DigestAndPercentileArrayStateSerializer
        implements AccumulatorStateSerializer<DigestAndPercentileArrayState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(DigestAndPercentileArrayState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            Slice digest = state.getDigest().serialize();

            SliceOutput output = Slices.allocate(
                    SIZE_OF_INT + // number of percentiles
                            state.getPercentiles().size() * SIZE_OF_DOUBLE + // percentiles
                            SIZE_OF_INT + // digest length
                            digest.length()) // digest
                    .getOutput();

            // write percentiles
            List<Double> percentiles = state.getPercentiles();
            output.appendInt(percentiles.size());
            for (double percentile : percentiles) {
                output.appendDouble(percentile);
            }

            output.appendInt(digest.length());
            output.appendBytes(digest);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, DigestAndPercentileArrayState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read number of percentiles
        int numPercentiles = input.readInt();

        ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();
        for (int i = 0; i < numPercentiles; i++) {
            percentilesListBuilder.add(input.readDouble());
        }
        state.setPercentiles(percentilesListBuilder.build());

        // read digest
        int length = input.readInt();
        state.setDigest(new QuantileDigest(input.readSlice(length)));
        state.addMemoryUsage(state.getDigest().estimatedInMemorySizeInBytes());
    }
}
