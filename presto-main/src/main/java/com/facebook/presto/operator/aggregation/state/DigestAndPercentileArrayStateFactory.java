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
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static java.util.Objects.requireNonNull;

public class DigestAndPercentileArrayStateFactory
        implements AccumulatorStateFactory<DigestAndPercentileArrayState>
{
    @Override
    public DigestAndPercentileArrayState createSingleState()
    {
        return new SingleDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getSingleStateClass()
    {
        return SingleDigestAndPercentileArrayState.class;
    }

    @Override
    public DigestAndPercentileArrayState createGroupedState()
    {
        return new GroupedDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileArrayState.class;
    }

    public static class GroupedDigestAndPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileArrayState
    {
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + digests.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class SingleDigestAndPercentileArrayState
            implements DigestAndPercentileArrayState
    {
        private QuantileDigest digest;
        private List<Double> percentiles;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
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
