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

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.AbstractGroupCollectionAggregationState;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;

public final class GroupArrayAggregationState
        extends AbstractGroupCollectionAggregationState<ArrayAggregationStateConsumer>
        implements ArrayAggregationState
{
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    private static final int VALUE_CHANNEL = 0;

    GroupArrayAggregationState(Type valueType)
    {
        super(PageBuilder.withMaxPageSize(MAX_BLOCK_SIZE, ImmutableList.of(valueType)));
    }

    @Override
    public final void add(Block block, int position)
    {
        prepareAdd();
        appendAtChannel(VALUE_CHANNEL, block, position);
    }

    @Override
    protected final void accept(ArrayAggregationStateConsumer consumer, PageBuilder pageBuilder, int currentPosition)
    {
        consumer.accept(pageBuilder.getBlockBuilder(VALUE_CHANNEL), currentPosition);
    }
}
