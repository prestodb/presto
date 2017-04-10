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

import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.stats.QuantileDigest;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static java.util.Objects.requireNonNull;

public class DigestAndPercentileStateFactory
        implements AccumulatorStateFactory<DigestAndPercentileState>
{
    @Override
    public DigestAndPercentileState createSingleState()
    {
        return new SingleDigestAndPercentileState();
    }

    @Override
    public Class<? extends DigestAndPercentileState> getSingleStateClass()
    {
        return SingleDigestAndPercentileState.class;
    }

    @Override
    public DigestAndPercentileState createGroupedState()
    {
        return new GroupedDigestAndPercentileState();
    }

    @Override
    public Class<? extends DigestAndPercentileState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileState.class;
    }

    public static class GroupedDigestAndPercentileState
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileState
    {
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            requireNonNull(digest, "value is null");
            digests.set(getGroupId(), digest);
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
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + digests.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class SingleDigestAndPercentileState
            implements DigestAndPercentileState
    {
        private QuantileDigest digest;
        private double percentile;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = digest;
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
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            if (digest == null) {
                return SIZE_OF_DOUBLE;
            }
            return digest.estimatedInMemorySizeInBytes() + SIZE_OF_DOUBLE;
        }
    }
}
