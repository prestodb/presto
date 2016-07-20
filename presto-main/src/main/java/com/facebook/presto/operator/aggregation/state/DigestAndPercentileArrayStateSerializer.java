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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

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
            DynamicSliceOutput sliceOutput =
                    new DynamicSliceOutput(state.getDigest().estimatedSerializedSizeInBytes() + (state.getPercentiles().size() * SIZE_OF_DOUBLE) +  SIZE_OF_LONG);
            // write digest
            state.getDigest().serialize(sliceOutput);
            // write percentiles
            List<Double> percentiles = state.getPercentiles();
            sliceOutput.appendLong(percentiles.size());
            for (Double percentile : percentiles) {
                sliceOutput.appendDouble(percentile);
            }

            VARBINARY.writeSlice(out, sliceOutput.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, DigestAndPercentileArrayState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read digest
        state.setDigest(QuantileDigest.deserialize(input));
        state.addMemoryUsage(state.getDigest().estimatedInMemorySizeInBytes());

        // read number of percentiles
        long numPercentiles = input.readLong();

        ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();
        for (int i = 0; i < numPercentiles; i++) {
            percentilesListBuilder.add(input.readDouble());
        }
        state.setPercentiles(percentilesListBuilder.build());
    }
}
