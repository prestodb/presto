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

import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxSplitSize;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HiveSplit.class);

    private final String queryId;
    private final String databaseName;
    private final String tableName;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final AsyncQueue<InternalHiveSplit> queue;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();
    private final int maxOutstandingSplitsBytes;

    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final AtomicInteger remainingInitialSplits;

    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final HiveSplitLoader splitLoader;
    private volatile boolean closed;

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();

    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();

    HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.queue = new AsyncQueue<>(maxOutstandingSplits, executor);
        this.maxOutstandingSplitsBytes = toIntExact(maxOutstandingSplitsSize.toBytes());
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");

        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
    }

    /**
     * The upper bound of outstanding split count.
     * It might be larger than the actual number when called concurrently with other methods.
     */
    @VisibleForTesting
    int getBufferedInternalSplitCount()
    {
        return bufferedInternalSplitCount.get();
    }

    CompletableFuture<?> addToQueue(List<? extends InternalHiveSplit> splits)
    {
        CompletableFuture<?> lastResult = completedFuture(null);
        for (InternalHiveSplit split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    CompletableFuture<?> addToQueue(InternalHiveSplit split)
    {
        if (throwable.get() == null) {
            if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
                // This limit should never be hit given there is a limit of maxOutstandingSplits.
                // If it's hit, it means individual splits are huge.
                if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                    highMemorySplitSourceCounter.update(1);
                    log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                            databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
                }
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format(
                        "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
            }
            bufferedInternalSplitCount.incrementAndGet();
            return toCompletableFuture(queue.offer(split));
        }
        return completedFuture(null);
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
        // The error must be recorded before setting the finish marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
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

        ListenableFuture<List<ConnectorSplit>> future = queue.borrowBatchAsync(maxSize, internalSplits -> {
            ImmutableList.Builder<InternalHiveSplit> splitsToInsertBuilder = ImmutableList.builder();
            ImmutableList.Builder<ConnectorSplit> resultBuilder = ImmutableList.builder();
            int removedEstimatedSizeInBytes = 0;
            for (InternalHiveSplit internalSplit : internalSplits) {
                long maxSplitBytes = maxSplitSize.toBytes();
                if (remainingInitialSplits.get() > 0) {
                    if (remainingInitialSplits.getAndDecrement() > 0) {
                        maxSplitBytes = maxInitialSplitSize.toBytes();
                    }
                }
                InternalHiveBlock block = internalSplit.currentBlock();
                long splitBytes;
                if (internalSplit.isSplittable()) {
                    splitBytes = min(maxSplitBytes, block.getEnd() - internalSplit.getStart());
                }
                else {
                    splitBytes = internalSplit.getEnd() - internalSplit.getStart();
                }

                resultBuilder.add(new HiveSplit(
                        databaseName,
                        tableName,
                        internalSplit.getPartitionName(),
                        internalSplit.getPath(),
                        internalSplit.getStart(),
                        splitBytes,
                        internalSplit.getFileSize(),
                        internalSplit.getSchema(),
                        internalSplit.getPartitionKeys(),
                        block.getAddresses(),
                        internalSplit.getBucketNumber(),
                        internalSplit.isForceLocalScheduling(),
                        (TupleDomain<HiveColumnHandle>) compactEffectivePredicate,
                        transformValues(internalSplit.getColumnCoercions(), HiveTypeName::toHiveType)));
                internalSplit.increaseStart(splitBytes);

                if (internalSplit.isDone()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
                }
                else {
                    splitsToInsertBuilder.add(internalSplit);
                }
            }
            estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);

            List<InternalHiveSplit> splitsToInsert = splitsToInsertBuilder.build();
            List<ConnectorSplit> result = resultBuilder.build();
            bufferedInternalSplitCount.addAndGet(splitsToInsert.size() - result.size());

            return new AsyncQueue.BorrowResult<>(splitsToInsert, result);
        });

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            return failedFuture(throwable.get());
        }

        return toCompletableFuture(future);
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
