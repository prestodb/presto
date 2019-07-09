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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.tdigest.TDigest;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.SizeOf.sizeOfDoubleArray;
import static java.util.Objects.requireNonNull;

public class TDigestAndPercentileStateFactory
        implements AccumulatorStateFactory<TDigestAndPercentileState>
{
    @Override
    public TDigestAndPercentileState createSingleState()
    {
        return new SingleTDigestAndPercentileState();
    }

    @Override
    public Class<? extends TDigestAndPercentileState> getSingleStateClass()
    {
        return SingleTDigestAndPercentileState.class;
    }

    @Override
    public TDigestAndPercentileState createGroupedState()
    {
        return new GroupedTDigestAndPercentileState();
    }

    @Override
    public Class<? extends TDigestAndPercentileState> getGroupedStateClass()
    {
        return GroupedTDigestAndPercentileState.class;
    }

    public static class GroupedTDigestAndPercentileState
            extends AbstractGroupedAccumulatorState
            implements TDigestAndPercentileState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTDigestAndPercentileState.class).instanceSize();
        private final ObjectBigArray<TDigest> digests = new ObjectBigArray<>();
        private List<Double> percentilesList;
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
        }

        @Override
        public TDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(TDigest digest)
        {
            if (digest == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "TDigest is null");
            }
            if (digests.get(getGroupId()) != null && digests.get(getGroupId()).getCompressionFactor() != digest.getCompressionFactor()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "TDigest compression factor must be constant");
            }
            digests.set(getGroupId(), digest);
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesList;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            if (percentiles == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentiles is null");
            }
            if (percentilesList != null && !percentilesList.equals(percentiles)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentiles must be constant");
            }
            percentilesList = percentiles;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE + size + digests.sizeOf();
            if (percentilesList != null) {
                estimatedSize += sizeOfDoubleArray(percentilesList.size());
            }
            return estimatedSize;
        }
    }

    public static class SingleTDigestAndPercentileState
            implements TDigestAndPercentileState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleTDigestAndPercentileState.class).instanceSize();
        private TDigest digest;
        private List<Double> percentiles;

        @Override
        public TDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(TDigest digest)
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
            if (percentiles == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentiles is null");
            }
            if (this.percentiles != null && !this.percentiles.equals(percentiles)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentiles must be constant");
            }
            this.percentiles = percentiles;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            if (percentiles != null) {
                estimatedSize += sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
