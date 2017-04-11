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

import com.facebook.presto.operator.aggregation.BlockComparator;
import com.facebook.presto.operator.aggregation.TypedHeap;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class MinMaxNStateSerializer
        implements AccumulatorStateSerializer<MinMaxNState>
{
    private final BlockComparator blockComparator;
    private final Type elementType;
    private final ArrayType arrayType;
    private final Type serializedType;

    public MinMaxNStateSerializer(BlockComparator blockComparator, Type elementType)
    {
        this.blockComparator = blockComparator;
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
        this.serializedType = new RowType(ImmutableList.of(BIGINT, arrayType), Optional.empty());
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MinMaxNState state, BlockBuilder out)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();
        BIGINT.writeLong(blockBuilder, heap.getCapacity());
        BlockBuilder elements = blockBuilder.beginBlockEntry();
        heap.writeAll(elements);
        blockBuilder.closeEntry();

        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, MinMaxNState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        int capacity = toIntExact(BIGINT.getLong(currentBlock, 0));
        Block heapBlock = arrayType.getObject(currentBlock, 1);
        TypedHeap heap = new TypedHeap(blockComparator, elementType, capacity);
        heap.addAll(heapBlock);
        state.setTypedHeap(heap);
    }
}
