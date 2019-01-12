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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SingleArrayAggregationState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleArrayAggregationState.class).instanceSize();
    private BlockBuilder blockBuilder;
    private final Type type;

    public SingleArrayAggregationState(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;
        if (blockBuilder != null) {
            estimatedSize += blockBuilder.getRetainedSizeInBytes();
        }
        return estimatedSize;
    }

    @Override
    public void add(Block block, int position)
    {
        if (blockBuilder == null) {
            blockBuilder = type.createBlockBuilder(null, 16);
        }
        type.appendTo(block, position, blockBuilder);
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
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
        if (blockBuilder == null) {
            return true;
        }
        verify(blockBuilder.getPositionCount() != 0);
        return false;
    }

    @Override
    public void reset()
    {
        blockBuilder = null;
    }
}
