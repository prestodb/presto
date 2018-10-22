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
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static java.util.Objects.requireNonNull;

public class SingleMultimapAggregationState
        implements MultimapAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleMultimapAggregationState.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;
    private final Type keyType;
    private final Type valueType;
    private BlockBuilder keyBlockBuilder;
    private BlockBuilder valueBlockBuilder;

    public SingleMultimapAggregationState(Type keyType, Type valueType)
    {
        this.keyType = requireNonNull(keyType);
        this.valueType = requireNonNull(valueType);
        keyBlockBuilder = keyType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        valueBlockBuilder = valueType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
    }

    @Override
    public void add(Block key, Block value, int position)
    {
        keyType.appendTo(key, position, keyBlockBuilder);
        valueType.appendTo(value, position, valueBlockBuilder);
    }

    @Override
    public void forEach(MultimapAggregationStateConsumer consumer)
    {
        for (int i = 0; i < keyBlockBuilder.getPositionCount(); i++) {
            consumer.accept(keyBlockBuilder, valueBlockBuilder, i);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return keyBlockBuilder.getPositionCount() == 0;
    }

    @Override
    public int getEntryCount()
    {
        return keyBlockBuilder.getPositionCount();
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + keyBlockBuilder.getRetainedSizeInBytes() + valueBlockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void reset()
    {
        // Single aggregation state is used as scratch state in group accumulator.
        // Thus reset() will be called for each group (via MultimapAggregationStateSerializer#deserialize)
        keyBlockBuilder = keyBlockBuilder.newBlockBuilderLike(null);
        valueBlockBuilder = valueBlockBuilder.newBlockBuilderLike(null);
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (isEmpty()) {
            out.appendNull();
        }
        else {
            IntList initialPositions = new IntArrayList();
            IntBigArray linkedList = new IntBigArray();
            linkedList.ensureCapacity(keyBlockBuilder.getPositionCount());

            {
                TypedSet keySet = new TypedSet(keyType, getEntryCount(), MultimapAggregationFunction.NAME);
                IntList currentPositions = new IntArrayList();
                for (int i = 0; i < keyBlockBuilder.getPositionCount(); i++) {
                    // Merge values of the same key into an array
                    linkedList.add(i, i);
                    if (!keySet.contains(keyBlockBuilder, i)) {
                        keySet.add(keyBlockBuilder, i);
                        initialPositions.add(i);
                        currentPositions.add(i);
                    }
                    else {
                        int position = keySet.positionOf(keyBlockBuilder, i);
                        int oldPosition = currentPositions.getInt(position);
                        currentPositions.set(position, i);
                        linkedList.set(oldPosition, i);
                    }
                }
            }

            // Write keys and value arrays into one Block
            BlockBuilder multimapBlockBuilder = out.beginBlockEntry();
            for (int i = 0; i < initialPositions.size(); i++) {
                int previousIndex = -1;
                int valueIndex = initialPositions.getInt(i);
                keyType.appendTo(keyBlockBuilder, initialPositions.getInt(i), multimapBlockBuilder);
                BlockBuilder valuesBuilder = multimapBlockBuilder.beginBlockEntry();
                while (previousIndex != valueIndex) {
                    valueType.appendTo(valueBlockBuilder, valueIndex, valuesBuilder);
                    previousIndex = valueIndex;
                    valueIndex = linkedList.get(valueIndex);
                }
                multimapBlockBuilder.closeEntry();
            }
            out.closeEntry();
        }
    }
}
