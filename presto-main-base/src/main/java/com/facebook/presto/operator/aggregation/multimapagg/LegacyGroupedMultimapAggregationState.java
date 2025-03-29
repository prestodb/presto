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

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.type.TypeUtils.expectedValueSize;

@Deprecated
public class LegacyGroupedMultimapAggregationState
        extends AbstractGroupedAccumulatorState
        implements MultimapAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LegacyGroupedMultimapAggregationState.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;
    private final Type keyType;
    private final Type valueType;
    private final ObjectBigArray<BlockBuilder> keyBlockBuilders = new ObjectBigArray<>();
    private final ObjectBigArray<BlockBuilder> valueBlockBuilders = new ObjectBigArray<>();
    private long size;

    public LegacyGroupedMultimapAggregationState(Type keyType, Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public void ensureCapacity(long size)
    {
        keyBlockBuilders.ensureCapacity(size);
        valueBlockBuilders.ensureCapacity(size);
    }

    @Override
    public void add(Block key, Block value, int position)
    {
        BlockBuilder keyBlockBuilder = keyBlockBuilders.get(getGroupId());
        BlockBuilder valueBlockBuilder = valueBlockBuilders.get(getGroupId());
        if (keyBlockBuilder != null) {
            size -= keyBlockBuilder.getRetainedSizeInBytes();
            size -= valueBlockBuilder.getRetainedSizeInBytes();
        }
        else {
            keyBlockBuilder = keyType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
            valueBlockBuilder = valueType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
            keyBlockBuilders.set(getGroupId(), keyBlockBuilder);
            valueBlockBuilders.set(getGroupId(), valueBlockBuilder);
        }
        keyType.appendTo(key, position, keyBlockBuilder);
        valueType.appendTo(value, position, valueBlockBuilder);
        size += keyBlockBuilder.getRetainedSizeInBytes();
        size += valueBlockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void forEach(MultimapAggregationStateConsumer consumer)
    {
        BlockBuilder keyBlockBuilder = keyBlockBuilders.get(getGroupId());
        BlockBuilder valueBlockBuilder = valueBlockBuilders.get(getGroupId());
        if (keyBlockBuilder == null) {
            return;
        }
        for (int i = 0; i < keyBlockBuilder.getPositionCount(); i++) {
            consumer.accept(keyBlockBuilder, valueBlockBuilder, i);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return keyBlockBuilders.get(getGroupId()) == null;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + keyBlockBuilders.sizeOf() + valueBlockBuilders.sizeOf() + size;
    }

    @Override
    public int getEntryCount()
    {
        return keyBlockBuilders.get(getGroupId()).getPositionCount();
    }
}
