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
package com.facebook.presto.operator.aggregation.multimapagg;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.operator.aggregation.multimapagg.GroupedMultimapAggregationState.KEY_CHANNEL;
import static com.facebook.presto.operator.aggregation.multimapagg.GroupedMultimapAggregationState.VALUE_CHANNEL;
import static java.util.Objects.requireNonNull;

public class MultimapAggregationStateSerializer
        implements AccumulatorStateSerializer<MultimapAggregationState>
{
    private final Type keyType;
    private final Type valueType;
    private final ArrayType arrayType;

    public MultimapAggregationStateSerializer(Type keyType, Type valueType)
    {
        this.keyType = requireNonNull(keyType);
        this.valueType = requireNonNull(valueType);
        this.arrayType = new ArrayType(RowType.withDefaultFieldNames(ImmutableList.of(valueType, keyType)));
    }

    @Override
    public Type getSerializedType()
    {
        return arrayType;
    }

    @Override
    public void serialize(MultimapAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
            return;
        }
        BlockBuilder entryBuilder = out.beginBlockEntry();
        state.forEach((keyBlock, valueBlock, position) -> {
            BlockBuilder rowBlockBuilder = entryBuilder.beginBlockEntry();
            valueType.appendTo(valueBlock, position, rowBlockBuilder);
            keyType.appendTo(keyBlock, position, rowBlockBuilder);
            entryBuilder.closeEntry();
        });
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, MultimapAggregationState state)
    {
        state.reset();
        ColumnarRow columnarRow = toColumnarRow(arrayType.getObject(block, index));
        Block keys = columnarRow.getField(KEY_CHANNEL);
        Block values = columnarRow.getField(VALUE_CHANNEL);
        for (int i = 0; i < columnarRow.getPositionCount(); i++) {
            state.add(keys, values, i);
        }
    }
}
