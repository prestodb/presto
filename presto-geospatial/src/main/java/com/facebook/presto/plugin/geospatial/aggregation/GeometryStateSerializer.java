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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;

public class GeometryStateSerializer
        implements AccumulatorStateSerializer<GeometryState>
{
    @Override
    public Type getSerializedType()
    {
        return GEOMETRY;
    }

    @Override
    public void serialize(GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, EsriGeometrySerde.serialize(state.getGeometry()));
        }
    }

    @Override
    public void deserialize(Block block, int index, GeometryState state)
    {
        long previousMemorySize = state.getGeometry() != null ? state.getGeometry().estimateMemorySize() : 0;
        state.setGeometry(EsriGeometrySerde.deserialize(GEOMETRY.getSlice(block, index)), previousMemorySize);
    }
}
