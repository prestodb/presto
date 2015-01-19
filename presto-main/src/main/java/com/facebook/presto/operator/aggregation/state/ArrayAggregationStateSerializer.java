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

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.ArrayList;

import jersey.repackaged.com.google.common.base.Throwables;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeJsonUtils;

public class ArrayAggregationStateSerializer
        implements AccumulatorStateSerializer<ArrayAggregationState>
{
    @Override
    public Type getSerializedType()
    {
        return VARCHAR;
    }

    @Override
    public void serialize(ArrayAggregationState state, BlockBuilder out)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput((int) state.getEstimatedSize());
        if (state.getArray() != null) {
            Slice s = ArrayType.toStackRepresentation(state.getArray());
            sliceOutput.writeInt(s.length());
            sliceOutput.writeBytes(s);
        }
        else {
            sliceOutput.writeInt(0);
        }

        Slice slice = sliceOutput.slice();
        out.writeBytes(slice, 0, slice.length());
        out.closeEntry();
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void deserialize(Block block, int index, ArrayAggregationState state)
    {
        SliceInput input = block.getSlice(index, 0, block.getLength(index)).getInput();

        int sliceLength = input.readInt();
        state.setArray(null);
        if (sliceLength > 0) {
            ArrayList<Object> values = new ArrayList<Object>();
            values.addAll(TypeJsonUtils.getObjectList(input.readSlice(sliceLength)));
            state.setArray(values);
        }
    }
}
