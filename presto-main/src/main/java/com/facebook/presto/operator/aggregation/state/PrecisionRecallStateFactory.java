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

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleHistogram;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class PrecisionRecallStateFactory
        implements AccumulatorStateFactory<PrecisionRecallState>
{
    @Override
    public PrecisionRecallState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends PrecisionRecallState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public PrecisionRecallState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends PrecisionRecallState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements PrecisionRecallState
    {
        private final ObjectBigArray<FixedDoubleHistogram> trueWeights = new ObjectBigArray<>();
        private final ObjectBigArray<FixedDoubleHistogram> falseWeights = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            trueWeights.ensureCapacity(size);
            falseWeights.ensureCapacity(size);
        }

        @Override
        public void setTrueWeights(FixedDoubleHistogram histogram)
        {
            requireNonNull(histogram, "histogram is null");

            FixedDoubleHistogram previous = getTrueWeights();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            trueWeights.set(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
        }

        @Override
        public FixedDoubleHistogram getTrueWeights()
        {
            return trueWeights.get(getGroupId());
        }

        @Override
        public void setFalseWeights(FixedDoubleHistogram histogram)
        {
            requireNonNull(histogram, "histogram is null");

            FixedDoubleHistogram previous = getFalseWeights();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            falseWeights.set(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
        }

        @Override
        public FixedDoubleHistogram getFalseWeights()
        {
            return falseWeights.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + trueWeights.sizeOf() + falseWeights.sizeOf();
        }
    }

    public static class SingleState
            implements PrecisionRecallState
    {
        private boolean nullSet;
        private FixedDoubleHistogram trueWeights;
        private FixedDoubleHistogram falseWeights;

        @Override
        public void setTrueWeights(FixedDoubleHistogram histogram)
        {
            requireNonNull(histogram, "histogram is null");
            trueWeights = histogram;
        }

        @Override
        public FixedDoubleHistogram getTrueWeights()
        {
            return trueWeights;
        }

        @Override
        public void setFalseWeights(FixedDoubleHistogram histogram)
        {
            requireNonNull(histogram, "histogram is null");
            falseWeights = histogram;
        }

        @Override
        public FixedDoubleHistogram getFalseWeights()
        {
            return falseWeights;
        }

        @Override
        public long getEstimatedSize()
        {
            if (trueWeights == null) {
                return 0;
            }
            return trueWeights.estimatedInMemorySize() + falseWeights.estimatedInMemorySize();
        }
    }
}
