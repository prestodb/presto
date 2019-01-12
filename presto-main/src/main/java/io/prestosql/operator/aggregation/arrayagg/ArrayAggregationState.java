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
import io.prestosql.spi.function.AccumulatorState;

public interface ArrayAggregationState
        extends AccumulatorState
{
    void add(Block block, int position);

    void forEach(ArrayAggregationStateConsumer consumer);

    boolean isEmpty();

    default void merge(ArrayAggregationState otherState)
    {
        otherState.forEach(this::add);
    }

    default void reset()
    {
        throw new UnsupportedOperationException();
    }
}
