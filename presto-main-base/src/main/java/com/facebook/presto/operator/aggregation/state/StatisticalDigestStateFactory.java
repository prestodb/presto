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
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.StatisticalDigest;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.Function;

import static com.facebook.presto.operator.aggregation.StatisticalDigestFactory.createStatisticalQuantileDigest;
import static com.facebook.presto.operator.aggregation.StatisticalDigestFactory.createStatisticalTDigest;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static java.util.Objects.requireNonNull;

public class StatisticalDigestStateFactory<T>
        implements AccumulatorStateFactory<StatisticalDigestState>
{
    private final Function<Slice, StatisticalDigest<T>> deserializer;

    public static StatisticalDigestStateFactory<TDigest> createTDigestFactory()
    {
        return new StatisticalDigestStateFactory<TDigest>((slice) -> createStatisticalTDigest(createTDigest(slice)));
    }

    public static StatisticalDigestStateFactory<QuantileDigest> createQuantileDigestFactory()
    {
        return new StatisticalDigestStateFactory<QuantileDigest>((slice) -> createStatisticalQuantileDigest(new QuantileDigest(slice)));
    }

    private StatisticalDigestStateFactory(Function<Slice, StatisticalDigest<T>> deserializer)
    {
        this.deserializer = deserializer;
    }

    @Override
    public StatisticalDigestState createSingleState()
    {
        return new SingleStatisticalDigestState(deserializer);
    }

    @Override
    public Class<? extends StatisticalDigestState> getSingleStateClass()
    {
        return SingleStatisticalDigestState.class;
    }

    @Override
    public StatisticalDigestState createGroupedState()
    {
        return new GroupedStatisticalDigestState(deserializer);
    }

    @Override
    public Class<? extends StatisticalDigestState> getGroupedStateClass()
    {
        return GroupedStatisticalDigestState.class;
    }

    public static class GroupedStatisticalDigestState<T>
            extends AbstractGroupedAccumulatorState
            implements StatisticalDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedStatisticalDigestState.class).instanceSize();
        private final ObjectBigArray<StatisticalDigest<T>> digests = new ObjectBigArray<>();
        private long size;
        private final Function<Slice, StatisticalDigest<T>> deserializer;

        public GroupedStatisticalDigestState(Function<Slice, StatisticalDigest<T>> deserializer)
        {
            this.deserializer = deserializer;
        }

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
        }

        @Override
        public StatisticalDigest getStatisticalDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setStatisticalDigest(StatisticalDigest value)
        {
            requireNonNull(value, "value is null");
            digests.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public StatisticalDigest deserialize(Slice slice)
        {
            return deserializer.apply(slice);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf();
        }
    }

    public static class SingleStatisticalDigestState<T>
            implements StatisticalDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleStatisticalDigestState.class).instanceSize();
        private final Function<Slice, StatisticalDigest<T>> deserializer;
        private StatisticalDigest<T> digest;

        public SingleStatisticalDigestState(Function<Slice, StatisticalDigest<T>> deserializer)
        {
            this.deserializer = deserializer;
        }

        @Override
        public StatisticalDigest getStatisticalDigest()
        {
            return digest;
        }

        @Override
        public void setStatisticalDigest(StatisticalDigest value)
        {
            digest = value;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public StatisticalDigest deserialize(Slice slice)
        {
            return deserializer.apply(slice);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }
    }
}
