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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class DifferentialEntropyStateFactory
        implements AccumulatorStateFactory<DifferentialEntropyState>
{
    @Override
    public DifferentialEntropyState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends DifferentialEntropyState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public DifferentialEntropyState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends DifferentialEntropyState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements DifferentialEntropyState
    {
        private ObjectBigArray<DifferentialEntropyStateStrategy> strategies = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            strategies.ensureCapacity(size);
        }

        @Override
        public void setStrategy(DifferentialEntropyStateStrategy strategy)
        {
            DifferentialEntropyStateStrategy previous = requireNonNull(strategy, "strategy is null");
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            strategies.set(getGroupId(), strategy);
            size += strategy.getEstimatedSize();
        }

        @Override
        public DifferentialEntropyStateStrategy getStrategy()
        {
            return strategies.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + strategies.sizeOf();
        }
    }

    public static class SingleState
            implements DifferentialEntropyState
    {
        private DifferentialEntropyStateStrategy strategy;

        @Override
        public void setStrategy(DifferentialEntropyStateStrategy strategy)
        {
            this.strategy = requireNonNull(strategy, "strategy is null");
        }

        @Override
        public DifferentialEntropyStateStrategy getStrategy()
        {
            return strategy;
        }

        @Override
        public long getEstimatedSize()
        {
            return strategy == null ? 0 : strategy.getEstimatedSize();
        }
    }
}
