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

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.array.DoubleBigArray;
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ApproxPercentileStateFactory
        implements AccumulatorStateFactory<ApproxPercentileState>
{
    @Override
    public ApproxPercentileState createSingleState()
    {
        return new SingleApproxPercentileState();
    }

    @Override
    public Class<? extends ApproxPercentileState> getSingleStateClass()
    {
        return SingleApproxPercentileState.class;
    }

    @Override
    public ApproxPercentileState createGroupedState()
    {
        return new GroupedApproxPercentileState();
    }

    @Override
    public Class<? extends ApproxPercentileState> getGroupedStateClass()
    {
        return GroupedApproxPercentileState.class;
    }

    public static class GroupedApproxPercentileState
            extends AbstractGroupedAccumulatorState
            implements ApproxPercentileState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedApproxPercentileState.class).instanceSize();
        private final ObjectBigArray<QuantileDigest[]> digestGroups = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digestGroups.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public QuantileDigest[] getDigests()
        {
            return digestGroups.get(getGroupId());
        }

        @Override
        public void setDigests(QuantileDigest[] digests)
        {
            requireNonNull(digests, "value is null");
            digestGroups.set(getGroupId(), digests);
        }

        @Override
        public void addDigests(Block block, long weight)
        {
            QuantileDigest[] digests = digestGroups.get(getGroupId());
            checkState(block.getPositionCount() == digests.length, "The sizes of inputs cannot be different");
            for (int i = 0; i < digests.length; i++) {
                if (!block.isNull(i)) {
                    addMemoryUsage(-digests[i].estimatedInMemorySizeInBytes());
                    digests[i].add(BIGINT.getLong(block, i), weight);
                    addMemoryUsage(digests[i].estimatedInMemorySizeInBytes());
                }
            }
        }

        @Override
        public double getPercentile()
        {
            return percentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double percentile)
        {
            percentiles.set(getGroupId(), percentile);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public long estimatedInMemorySizeInBytes()
        {
            long estimatedInMemorySizeInBytes = 0L;
            QuantileDigest[] digests = digestGroups.get(getGroupId());
            for (int i = 0; i < digests.length; i++) {
                estimatedInMemorySizeInBytes += digests[i].estimatedInMemorySizeInBytes();
            }
            return estimatedInMemorySizeInBytes;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digestGroups.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class SingleApproxPercentileState
            implements ApproxPercentileState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleApproxPercentileState.class).instanceSize();
        private QuantileDigest[] digests;
        private double percentile;

        @Override
        public QuantileDigest[] getDigests()
        {
            return digests;
        }

        @Override
        public void setDigests(QuantileDigest[] digests)
        {
            this.digests = digests;
        }

        @Override
        public void addDigests(Block block, long weight)
        {
            checkState(block.getPositionCount() == digests.length, "The sizes of inputs cannot be different");
            for (int i = 0; i < digests.length; i++) {
                if (!block.isNull(i)) {
                    addMemoryUsage(-digests[i].estimatedInMemorySizeInBytes());
                    digests[i].add(BIGINT.getLong(block, i), weight);
                    addMemoryUsage(digests[i].estimatedInMemorySizeInBytes());
                }
            }
        }

        @Override
        public double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public long estimatedInMemorySizeInBytes()
        {
            long estimatedInMemorySizeInBytes = 0L;
            for (int i = 0; i < digests.length; i++) {
                estimatedInMemorySizeInBytes += digests[i].estimatedInMemorySizeInBytes();
            }
            return estimatedInMemorySizeInBytes;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digests != null && digests.length > 0) {
                for (QuantileDigest digest : digests) {
                    estimatedSize += digest.estimatedInMemorySizeInBytes();
                }
            }
            return estimatedSize;
        }
    }
}
