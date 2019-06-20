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

import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;

public class StatisticalTDigest
        implements StatisticalDigest<TDigest>
{
    private final TDigest tDigest;

    public StatisticalTDigest(TDigest tDigest)
    {
        this.tDigest = tDigest;
    }

    @Override
    public void add(Number value, long weight)
    {
        tDigest.add(value.doubleValue(), weight);
    }

    @Override
    public void merge(StatisticalDigest other)
    {
        StatisticalTDigest toMerge = (StatisticalTDigest) other;
        tDigest.merge(toMerge.tDigest);
    }

    @Override
    public Number getQuantile(double quantile)
    {
        return tDigest.getQuantile(quantile);
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
    public StatisticalDigest getDigest()
    {
        return new StatisticalTDigest(tDigest);
    }
}
