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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkNotNull;

public class SampledSplitSource
        implements SplitSource
{
    private final SplitSource splitSource;
    private final double sampleRatio;

    public SampledSplitSource(SplitSource splitSource, double sampleRatio)
    {
        this.splitSource = checkNotNull(splitSource, "dataSource is null");
        this.sampleRatio = sampleRatio;
    }

    @Nullable
    @Override
    public String getDataSourceName()
    {
        return splitSource.getDataSourceName();
    }

    @Override
    public List<Split> getNextBatch(int maxSize)
            throws InterruptedException
    {
        List<Split> nextBatch = splitSource.getNextBatch(maxSize);
        Iterable<Split> sampleIterable = Iterables.filter(nextBatch, new Predicate<Split>()
        {
            public boolean apply(@Nullable Split input)
            {
                return ThreadLocalRandom.current().nextDouble() < sampleRatio;
            }
        });
        return ImmutableList.copyOf(sampleIterable);
    }

    @Override
    public boolean isFinished()
    {
        return splitSource.isFinished();
    }
}
