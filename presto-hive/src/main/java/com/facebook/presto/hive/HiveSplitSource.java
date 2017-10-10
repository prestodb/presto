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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private final String connectorId;
    private final String databaseName;
    private final String tableName;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final AsyncQueue<InternalHiveSplit> queue;
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final HiveSplitLoader splitLoader;
    private volatile boolean closed;

    HiveSplitSource(
            String connectorId,
            String databaseName,
            String tableName,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            int maxOutstandingSplits,
            HiveSplitLoader splitLoader,
            Executor executor)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.queue = new AsyncQueue<>(maxOutstandingSplits, executor);
        this.splitLoader = splitLoader;
    }

    @VisibleForTesting
    int getOutstandingSplitCount()
    {
        return queue.size();
    }

    CompletableFuture<?> addToQueue(Iterator<? extends InternalHiveSplit> splits)
    {
        CompletableFuture<?> lastResult = CompletableFuture.completedFuture(null);
        while (splits.hasNext()) {
            InternalHiveSplit split = splits.next();
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    CompletableFuture<?> addToQueue(InternalHiveSplit split)
    {
        if (throwable.get() == null) {
            return queue.offer(split);
        }
        return CompletableFuture.completedFuture(null);
    }

    void noMoreSplits()
    {
        if (throwable.get() == null) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queue.finish();
        }
    }

    void fail(Throwable e)
    {
        // only record the first error message
        if (throwable.compareAndSet(null, e)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queue.finish();
        }
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        checkState(!closed, "Provider is already closed");

        CompletableFuture<List<ConnectorSplit>> future = queue.getBatchAsync(maxSize).thenApply(internalSplits -> {
            ImmutableList.Builder<ConnectorSplit> result = ImmutableList.builder();
            for (InternalHiveSplit internalSplit : internalSplits) {
                result.add(new HiveSplit(
                        connectorId,
                        databaseName,
                        tableName,
                        internalSplit.getPartitionName(),
                        internalSplit.getPath(),
                        internalSplit.getStart(),
                        internalSplit.getLength(),
                        internalSplit.getFileSize(),
                        internalSplit.getSchema(),
                        internalSplit.getPartitionKeys(),
                        internalSplit.getAddresses(),
                        internalSplit.getBucketNumber(),
                        internalSplit.isForceLocalScheduling(),
                        (TupleDomain<HiveColumnHandle>) compactEffectivePredicate,
                        transformValues(internalSplit.getColumnCoercions(), HiveTypeName::toHiveType)));
            }
            return result.build();
        });

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            return failedFuture(throwable.get());
        }

        return future;
    }

    @Override
    public boolean isFinished()
    {
        // the finished marker must be checked before checking the throwable
        // to avoid a race with the fail method
        boolean isFinished = queue.isFinished();
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }
        return isFinished;
    }

    @Override
    public void close()
    {
        // Stop the split loader before finishing the queue.
        // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
        // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
        splitLoader.stop();
        queue.finish();

        closed = true;
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }
}
