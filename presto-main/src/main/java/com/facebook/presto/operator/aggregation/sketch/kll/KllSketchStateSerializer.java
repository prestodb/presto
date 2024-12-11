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
package com.facebook.presto.operator.aggregation.sketch.kll;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.TypeParameter;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.memory.Memory;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.sketch.kll.KllSketchAggregationState.getEstimatedKllInMemorySize;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

public class KllSketchStateSerializer
        implements AccumulatorStateSerializer<KllSketchAggregationState>
{
    private final Type type;

    public KllSketchStateSerializer(@TypeParameter("T") Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(KllSketchAggregationState state, BlockBuilder out)
    {
        KllSketchWithKAggregationFunction.output(state, out);
    }

    @Override
    public void deserialize(Block block, int index, KllSketchAggregationState state)
    {
        if (block.isNull(index)) {
            state.setSketch(null);
            return;
        }
        Memory memory = Memory.wrap(VARBINARY.getSlice(block, index).toByteBuffer(), LITTLE_ENDIAN);
        KllSketchAggregationState.SketchParameters parameters = KllSketchAggregationState.getSketchParameters(type);
        // use heapify over wrap in order to get a writable sketch for updates and merges
        KllItemsSketch sketch = KllItemsSketch.heapify(memory, parameters.getComparator(), parameters.getSerde());
        state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(state.getSketch(), type.getJavaType()));
        state.setSketch(sketch);
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(state.getSketch(), type.getJavaType()));
    }
}
