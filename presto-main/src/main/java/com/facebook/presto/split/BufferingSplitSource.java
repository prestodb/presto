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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class BufferingSplitSource
        implements SplitSource
{
    private final int bufferSize;
    private final SplitSource source;

    public BufferingSplitSource(SplitSource source, int bufferSize)
    {
        this.source = requireNonNull(source, "source is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public ConnectorId getConnectorId()
    {
        return source.getConnectorId();
    }

    @Override
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return source.getTransactionHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        return GetNextBatch.fetchNextBatchAsync(source, Math.min(bufferSize, maxSize), maxSize, partitionHandle, lifespan);
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    private static class GetNextBatch
    {
        private final SplitSource splitSource;
        private final int min;
        private final int max;
        private final ConnectorPartitionHandle partitionHandle;
        private final Lifespan lifespan;

        private final List<Split> splits = new ArrayList<>();
        private boolean noMoreSplits;

        public static ListenableFuture<SplitBatch> fetchNextBatchAsync(
                SplitSource splitSource,
                int min,
                int max,
                ConnectorPartitionHandle partitionHandle,
                Lifespan lifespan)
        {
            GetNextBatch getNextBatch = new GetNextBatch(splitSource, min, max, partitionHandle, lifespan);
            ListenableFuture<?> future = getNextBatch.fetchSplits();
            return Futures.transform(future, ignored -> new SplitBatch(getNextBatch.splits, getNextBatch.noMoreSplits), directExecutor());
        }

        private GetNextBatch(SplitSource splitSource, int min, int max, ConnectorPartitionHandle partitionHandle, Lifespan lifespan)
        {
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            checkArgument(min <= max, "Min splits greater than max splits");
            this.min = min;
            this.max = max;
            this.partitionHandle = requireNonNull(partitionHandle, "partitionHandle is null");
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
        }

        private ListenableFuture<?> fetchSplits()
        {
            if (splits.size() >= min) {
                return immediateFuture(null);
            }
            ListenableFuture<SplitBatch> future = splitSource.getNextBatch(partitionHandle, lifespan, max - splits.size());
            return Futures.transformAsync(future, splitBatch -> {
                splits.addAll(splitBatch.getSplits());
                if (splitBatch.isLastBatch()) {
                    noMoreSplits = true;
                    return immediateFuture(null);
                }
                return fetchSplits();
            }, directExecutor());
        }
    }
}
