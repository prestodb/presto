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

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.ShortBigArray;
import com.facebook.presto.operator.aggregation.AbstractGroupCollectionAggregationState;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class GroupedMultimapAggregationState
        extends AbstractGroupCollectionAggregationState<MultimapAggregationStateConsumer>
        implements MultimapAggregationState
{
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    static final int VALUE_CHANNEL = 0;
    static final int KEY_CHANNEL = 1;
    private final Type keyType;
    private final Type valueType;

    public GroupedMultimapAggregationState(Type keyType, Type valueType)
    {
        super(PageBuilder.withMaxPageSize(MAX_BLOCK_SIZE, ImmutableList.of(valueType, keyType)));
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public final void add(Block keyBlock, Block valueBlock, int position)
    {
        prepareAdd();
        appendAtChannel(VALUE_CHANNEL, valueBlock, position);
        appendAtChannel(KEY_CHANNEL, keyBlock, position);
    }

    @Override
    protected final void accept(MultimapAggregationStateConsumer consumer, PageBuilder pageBuilder, int currentPosition)
    {
        consumer.accept(pageBuilder.getBlockBuilder(KEY_CHANNEL), pageBuilder.getBlockBuilder(VALUE_CHANNEL), currentPosition);
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (isEmpty()) {
            out.appendNull();
        }
        else {
            IntList initialPositions = new IntArrayList();
            ShortBigArray blockIndices = new ShortBigArray();
            blockIndices.ensureCapacity(getEntryCount());
            IntBigArray positionIndices = new IntBigArray();
            positionIndices.ensureCapacity(getEntryCount());
            IntBigArray linkedList = new IntBigArray();
            linkedList.ensureCapacity(getEntryCount());

            {
                TypedSet keySet = new TypedSet(keyType, getEntryCount(), MultimapAggregationFunction.NAME);
                IntList currentPositions = new IntArrayList();

                short currentBlockId = getHeadBlockIndex();
                int currentPosition = getHeadPosition();
                for (int i = 0; currentBlockId != NULL; i++) {
                    PageBuilder page = getPageBuilder(currentBlockId);
                    BlockBuilder keys = page.getBlockBuilder(KEY_CHANNEL);

                    // Merge values of the same key into an array
                    linkedList.add(i, i);
                    if (!keySet.contains(keys, currentPosition)) {
                        keySet.add(keys, currentPosition);
                        initialPositions.add(i);
                        currentPositions.add(i);
                    }
                    else {
                        int position = keySet.positionOf(keys, currentPosition);
                        int oldPosition = currentPositions.getInt(position);
                        currentPositions.set(position, i);
                        linkedList.set(oldPosition, i);
                    }

                    blockIndices.set(i, currentBlockId);
                    positionIndices.set(i, currentPosition);
                    long absoluteCurrentAddress = toAbsolutePosition(currentBlockId, currentPosition);
                    currentBlockId = getNextBlockIndex(absoluteCurrentAddress);
                    currentPosition = getNextPosition(absoluteCurrentAddress);
                }
            }

            // Write keys and value arrays into one Block
            BlockBuilder multimapBlockBuilder = out.beginBlockEntry();
            for (int i = 0; i < initialPositions.size(); i++) {
                int previousIndex = -1;
                int valueIndex = initialPositions.getInt(i);
                short currentBlockId = blockIndices.get(valueIndex);
                int currentPosition = positionIndices.get(valueIndex);
                PageBuilder page = getPageBuilder(currentBlockId);

                keyType.appendTo(page.getBlockBuilder(KEY_CHANNEL), currentPosition, multimapBlockBuilder);
                BlockBuilder valuesBuilder = multimapBlockBuilder.beginBlockEntry();
                while (previousIndex != valueIndex) {
                    currentPosition = positionIndices.get(valueIndex);
                    currentBlockId = blockIndices.get(valueIndex);
                    valueType.appendTo(getPageBuilder(currentBlockId).getBlockBuilder(VALUE_CHANNEL), currentPosition, valuesBuilder);
                    previousIndex = valueIndex;
                    valueIndex = linkedList.get(valueIndex);
                }
                multimapBlockBuilder.closeEntry();
            }
            out.closeEntry();
        }
    }
}
