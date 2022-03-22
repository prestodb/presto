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
package com.facebook.presto.operator.aggregation;

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

public class StatisticalDigestFactory
{
    private StatisticalDigestFactory()
    {
    }

    public static StatisticalDigest createStatisticalTDigest(TDigest tDigest)
    {
        return new StatisticalTDigest(tDigest);
    }

    public static StatisticalDigest createStatisticalQuantileDigest(QuantileDigest quantileDigest)
    {
        return new StatisticalQuantileDigest(quantileDigest);
    }

    public static class StatisticalTDigest
            implements StatisticalDigest<TDigest>
    {
        private final TDigest tDigest;

        public StatisticalTDigest(TDigest tDigest)
        {
            this.tDigest = tDigest;
        }

        @Override
        public void add(double value, long weight)
        {
            tDigest.add(value, weight);
        }

        @Override
        public void merge(StatisticalDigest other)
        {
            checkArgument(other instanceof StatisticalTDigest);
            StatisticalTDigest toMerge = (StatisticalTDigest) other;
            tDigest.merge(toMerge.tDigest);
        }

        @Override
        public double getSize()
        {
            return tDigest.getSize();
        }

        @Override
        public long estimatedInMemorySizeInBytes()
        {
            return tDigest.estimatedInMemorySizeInBytes();
        }

        @Override
        public Slice serialize()
        {
            return tDigest.serialize();
        }

        @Override
        public StatisticalDigest<TDigest> getDigest()
        {
            return this;
        }
    }

    public static class StatisticalQuantileDigest
            implements StatisticalDigest<QuantileDigest>
    {
        private final QuantileDigest qdigest;

        public StatisticalQuantileDigest(QuantileDigest qdigest)
        {
            this.qdigest = qdigest;
        }

        @Override
        public void add(long value, long weight)
        {
            qdigest.add(value, weight);
        }

        @Override
        public void merge(StatisticalDigest other)
        {
            checkArgument(other instanceof StatisticalQuantileDigest);
            StatisticalQuantileDigest toMerge = (StatisticalQuantileDigest) other;
            this.qdigest.merge(toMerge.qdigest);
        }

        @Override
        public double getSize()
        {
            return qdigest.getCount();
        }

        @Override
        public long estimatedInMemorySizeInBytes()
        {
            return qdigest.estimatedInMemorySizeInBytes();
        }

        @Override
        public Slice serialize()
        {
            return qdigest.serialize();
        }

        @Override
        public StatisticalDigest<QuantileDigest> getDigest()
        {
            return this;
        }
    }
}
