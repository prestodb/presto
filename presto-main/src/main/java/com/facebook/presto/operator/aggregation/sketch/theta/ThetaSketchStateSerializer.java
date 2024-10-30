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
package com.facebook.presto.operator.aggregation.sketch.theta;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.slice.Slice;
import com.facebook.slice.Slices;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Verify.verify;
import static org.apache.datasketches.common.Family.UNION;

public class ThetaSketchStateSerializer
        implements AccumulatorStateSerializer<ThetaSketchAggregationState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(ThetaSketchAggregationState state, BlockBuilder out)
    {
        Slice stateMemory = Slices.wrappedBuffer(state.getSketch().toByteArray());
        VARBINARY.writeSlice(out, stateMemory);
    }

    @Override
    public void deserialize(Block block, int index, ThetaSketchAggregationState state)
    {
        Slice data = VARBINARY.getSlice(block, index);
        SetOperation op = Union.wrap(WritableMemory.writableWrap(data.getBytes()));
        verify(op.getFamily() == UNION);
        state.setSketch((Union) op);
    }
}
