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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncSplitSource<T>
        implements ConnectorSplitSource
{
    private final CloseableIterator<T> iterator;
    private final Function<T, ? extends ConnectorSplit> toSplit;
    private final Executor executor;

    @GuardedBy("this")
    private CompletableFuture<ConnectorSplitBatch> future;

    public AsyncSplitSource(CloseableIterator<T> iterator, Function<T, ? extends ConnectorSplit> toSplit, Executor executor)
    {
        this.iterator = new SynchronizedCloseableIterator<>(iterator);
        this.toSplit = requireNonNull(toSplit, "toSplit is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public synchronized CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partition, int maxSize)
    {
        checkState((future == null) || future.isDone(), "previous batch not completed");
        future = supplyAsync(batchSupplier(maxSize), executor);
        return future;
    }

    @Override
    public synchronized boolean isFinished()
    {
        checkState((future == null) || future.isDone(), "previous batch not completed");
        return !iterator.hasNext();
    }

    @Override
    public synchronized void close()
    {
        if (future != null) {
            future.cancel(true);
            future = null;
        }
        executor.execute(iterator::close);
    }

    private Supplier<ConnectorSplitBatch> batchSupplier(int maxSize)
    {
        return () -> {
            ImmutableList.Builder<ConnectorSplit> list = ImmutableList.builder();
            for (int i = 0; i < maxSize; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Split batch fetch was interrupted");
                }
                if (!iterator.hasNext()) {
                    break;
                }
                list.add(toSplit.apply(iterator.next()));
            }
            return new ConnectorSplitBatch(list.build(), !iterator.hasNext());
        };
    }
}
