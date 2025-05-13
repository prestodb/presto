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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.approxmostfrequent.stream.StreamSummary;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class ApproximateMostFrequentStateFactory
        implements AccumulatorStateFactory<ApproximateMostFrequentState>
{
    @Override
    public ApproximateMostFrequentState createSingleState()
    {
        return new SingleApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentState> getSingleStateClass()
    {
        return SingleApproximateMostFrequentState.class;
    }

    @Override
    public ApproximateMostFrequentState createGroupedState()
    {
        return new GroupedApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentState> getGroupedStateClass()
    {
        return GroupedApproximateMostFrequentState.class;
    }

    public static class SingleApproximateMostFrequentState
            implements ApproximateMostFrequentState
    {
        private StreamSummary streamSummary;
        private long size;

        @Override
        public StreamSummary getStateSummary()
        {
            return streamSummary;
        }

        @Override
        public void setStateSummary(StreamSummary histogram)
        {
            this.streamSummary = histogram;
            size = histogram.estimatedInMemorySize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size;
        }
    }

    public static class GroupedApproximateMostFrequentState
            extends AbstractGroupedAccumulatorState
            implements ApproximateMostFrequentState
    {
        private final ObjectBigArray<StreamSummary> streamSummaries = new ObjectBigArray<>();
        private long size;

        @Override
        public StreamSummary getStateSummary()
        {
            return streamSummaries.get(getGroupId());
        }

        @Override
        public void setStateSummary(StreamSummary histogram)
        {
            StreamSummary previous = getStateSummary();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            streamSummaries.set(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
        }

        @Override
        public void ensureCapacity(long size)
        {
            streamSummaries.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + streamSummaries.sizeOf();
        }
    }
}
