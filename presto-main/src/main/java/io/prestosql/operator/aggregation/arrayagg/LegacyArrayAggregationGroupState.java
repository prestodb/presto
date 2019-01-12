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
package io.prestosql.operator.aggregation.arrayagg;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Verify.verify;

// one BlockBuilder per group, which causes GC pressure and excessive cross region references.
@Deprecated
public class LegacyArrayAggregationGroupState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LegacyArrayAggregationGroupState.class).instanceSize();
    private final ObjectBigArray<BlockBuilder> blockBuilders;
    private final Type type;
    private long size;

    public LegacyArrayAggregationGroupState(Type type)
    {
        this.type = type;
        this.blockBuilders = new ObjectBigArray<>();
        this.size = 0;
    }

    @Override
    public void ensureCapacity(long size)
    {
        blockBuilders.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + blockBuilders.sizeOf();
    }

    @Override
    public void add(Block block, int position)
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        long startSize = 0;
        if (blockBuilder == null) {
            blockBuilder = type.createBlockBuilder(null, 4);
            blockBuilders.set(getGroupId(), blockBuilder);
        }
        else {
            startSize = blockBuilder.getRetainedSizeInBytes();
        }
        type.appendTo(block, position, blockBuilder);
        size += blockBuilder.getRetainedSizeInBytes() - startSize;
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        if (blockBuilder == null) {
            return;
        }

        for (int i = 0; i < blockBuilder.getPositionCount(); i++) {
            consumer.accept(blockBuilder, i);
        }
    }

    @Override
    public boolean isEmpty()
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        verify(blockBuilder.getPositionCount() != 0);
        return false;
    }
}
