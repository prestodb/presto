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
package com.facebook.presto.split;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SampledSplitSource
        implements SplitSource
{
    private final SplitSource splitSource;
    private final double sampleRatio;

    public SampledSplitSource(SplitSource splitSource, double sampleRatio)
    {
        this.splitSource = requireNonNull(splitSource, "dataSource is null");
        this.sampleRatio = sampleRatio;
    }

    @Nullable
    @Override
    public ConnectorId getConnectorId()
    {
        return splitSource.getConnectorId();
    }

    @Override
    public ListenableFuture<List<Split>> getNextBatch(int maxSize)
    {
        ListenableFuture<List<Split>> batch = splitSource.getNextBatch(maxSize);
        return Futures.transform(batch, splits -> splits.stream()
                .filter(input -> ThreadLocalRandom.current().nextDouble() < sampleRatio)
                .collect(toImmutableList()));
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    @Override
    public boolean isFinished()
    {
        return splitSource.isFinished();
    }
}
