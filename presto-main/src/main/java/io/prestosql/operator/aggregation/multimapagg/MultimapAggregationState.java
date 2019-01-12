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
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateFactoryClass = MultimapAggregationStateFactory.class, stateSerializerClass = MultimapAggregationStateSerializer.class)
public interface MultimapAggregationState
        extends AccumulatorState
{
    void add(Block keyBlock, Block valueBlock, int position);

    void forEach(MultimapAggregationStateConsumer consumer);

    default void merge(MultimapAggregationState otherState)
    {
        otherState.forEach(this::add);
    }

    boolean isEmpty();

    default void reset()
    {
        throw new UnsupportedOperationException();
    }

    long getEstimatedSize();

    int getEntryCount();
}
