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
package com.facebook.presto.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;

import static com.facebook.presto.geospatial.serde.GeometrySerde.readEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerde.writeEnvelope;
import static com.facebook.presto.plugin.geospatial.SpatialPartitioningType.SPATIAL_PARTITIONING;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class SpatialPartitioningStateSerializer
        implements AccumulatorStateSerializer<SpatialPartitioningState>
{
    @Override
    public Type getSerializedType()
    {
        return SPATIAL_PARTITIONING;
    }

    @Override
    public void serialize(SpatialPartitioningState state, BlockBuilder out)
    {
        int count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            DynamicSliceOutput output = new DynamicSliceOutput(SIZE_OF_INT + 4 * SIZE_OF_DOUBLE);
            output.writeInt(count);
            writeEnvelope(state.getEnvelope(), output);

            for (Envelope sample : state.getSamples()) {
                writeEnvelope(sample, output);
            }

            SPATIAL_PARTITIONING.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, SpatialPartitioningState state)
    {
        if (block.isNull(index)) {
            return;
        }

        BasicSliceInput input = SPATIAL_PARTITIONING.getSlice(block, index).getInput();
        state.setCount(input.readInt());
        state.setEnvelope(readEnvelope(input));

        while (input.available() > 0) {
            state.getSamples().add(readEnvelope(input));
        }
    }
}
