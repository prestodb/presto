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
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class DigestAndPercentileArrayStateSerializer
        implements AccumulatorStateSerializer<DigestAndPercentileArrayState>
{
    @Override
    public Type getSerializedType()
    {
        return VARCHAR;
    }

    @Override
    public void serialize(DigestAndPercentileArrayState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(state.getDigest().estimatedSerializedSizeInBytes() + SIZE_OF_DOUBLE);
            // write digest
            state.getDigest().serialize(sliceOutput);
            // write percentiles
            List<Double> percentiles = state.getPercentiles();
            sliceOutput.appendLong(percentiles.size());
            for (Double percentile : percentiles) {
                sliceOutput.appendDouble(percentile);
            }

            Slice slice = sliceOutput.slice();
            VARCHAR.writeSlice(out, slice);
        }
    }

    @Override
    public void deserialize(Block block, int index, DigestAndPercentileArrayState state)
    {
        if (!block.isNull(index)) {
            SliceInput input = VARCHAR.getSlice(block, index).getInput();

            // read digest
            state.setDigest(QuantileDigest.deserialize(input));
            state.addMemoryUsage(state.getDigest().estimatedInMemorySizeInBytes());

            // read number of percentiles
            Long numPercentiles = input.readLong();

            List<Double> percentiles = new ArrayList<Double>(numPercentiles.intValue());
            for (int i = 0; i < numPercentiles; i++) {
                percentiles.add(input.readDouble());
            }
            state.setPercentiles(percentiles);
        }
    }
}
