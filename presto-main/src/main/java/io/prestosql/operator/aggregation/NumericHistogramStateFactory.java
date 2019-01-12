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
package io.prestosql.operator.aggregation;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class NumericHistogramStateFactory
        implements AccumulatorStateFactory<DoubleHistogramAggregation.State>
{
    @Override
    public DoubleHistogramAggregation.State createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends DoubleHistogramAggregation.State> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public DoubleHistogramAggregation.State createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends DoubleHistogramAggregation.State> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements DoubleHistogramAggregation.State
    {
        private final ObjectBigArray<NumericHistogram> histograms = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            histograms.ensureCapacity(size);
        }

        @Override
        public NumericHistogram get()
        {
            return histograms.get(getGroupId());
        }

        @Override
        public void set(NumericHistogram value)
        {
            requireNonNull(value, "value is null");

            NumericHistogram previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            histograms.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size + histograms.sizeOf();
        }
    }

    public static class SingleState
            implements DoubleHistogramAggregation.State
    {
        private NumericHistogram histogram;

        @Override
        public NumericHistogram get()
        {
            return histogram;
        }

        @Override
        public void set(NumericHistogram value)
        {
            histogram = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (histogram == null) {
                return 0;
            }
            return histogram.estimatedInMemorySize();
        }
    }
}
