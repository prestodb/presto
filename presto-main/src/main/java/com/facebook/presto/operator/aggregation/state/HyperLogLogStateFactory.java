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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.stats.cardinality.HyperLogLog;

import static java.util.Objects.requireNonNull;

public class HyperLogLogStateFactory
        implements AccumulatorStateFactory<HyperLogLogState>
{
    @Override
    public HyperLogLogState createSingleState()
    {
        return new SingleHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getSingleStateClass()
    {
        return SingleHyperLogLogState.class;
    }

    @Override
    public HyperLogLogState createGroupedState()
    {
        return new GroupedHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getGroupedStateClass()
    {
        return GroupedHyperLogLogState.class;
    }

    public static class GroupedHyperLogLogState
            extends AbstractGroupedAccumulatorState
            implements HyperLogLogState
    {
        private final ObjectBigArray<HyperLogLog> hlls = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            hlls.ensureCapacity(size);
        }

        @Override
        public HyperLogLog getHyperLogLog()
        {
            return hlls.get(getGroupId());
        }

        @Override
        public void setHyperLogLog(HyperLogLog value)
        {
            requireNonNull(value, "value is null");
            hlls.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + hlls.sizeOf();
        }
    }

    public static class SingleHyperLogLogState
            implements HyperLogLogState
    {
        private HyperLogLog hll;

        @Override
        public HyperLogLog getHyperLogLog()
        {
            return hll;
        }

        @Override
        public void setHyperLogLog(HyperLogLog value)
        {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            if (hll == null) {
                return 0;
            }
            return hll.estimatedInMemorySize();
        }
    }
}
