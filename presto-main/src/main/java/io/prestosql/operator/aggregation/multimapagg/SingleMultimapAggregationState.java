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
package io.prestosql.operator.aggregation.multimapagg;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static io.prestosql.type.TypeUtils.expectedValueSize;
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
}
