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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hive.util.AsyncQueue.BorrowResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.MoreFutures.failedFuture;
import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxSplitSize;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.CLOSED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.FAILED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.INITIAL;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.NO_MORE_SPLITS;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HiveSplit.class);

    private final String queryId;
    private final String databaseName;
    private final String tableName;
    private final CacheQuotaRequirement cacheQuotaRequirement;
    private final PerBucket queues;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();
    private final long maxOutstandingSplitsBytes;

    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final boolean useRewindableSplitSource;
    private final AtomicInteger remainingInitialSplits;

    private final HiveSplitLoader splitLoader;
    private final AtomicReference<State> stateReference = new AtomicReference<>(State.initial());

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();

    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();

    private HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            CacheQuotaRequirement cacheQuotaRequirement,
            PerBucket queues,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            CounterStat highMemorySplitSourceCounter,
            boolean useRewindableSplitSource)
    {
        requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.cacheQuotaRequirement = requireNonNull(cacheQuotaRequirement, "cacheQuotaRequirement is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queues = requireNonNull(queues, "queues is null");
        this.maxOutstandingSplitsBytes = requireNonNull(maxOutstandingSplitsSize, "maxOutstandingSplitsSize is null").toBytes();
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");

        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.useRewindableSplitSource = useRewindableSplitSource;
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
    }

    public static HiveSplitSource allAtOnce(
            ConnectorSession session,
            String databaseName,
            String tableName,
            CacheQuotaRequirement cacheQuotaRequirement,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                cacheQuotaRequirement,
                new PerBucket()
                {
                    private final AsyncQueue<InternalHiveSplit> queue = new AsyncQueue<>(maxOutstandingSplits, executor);

                    @Override
                    public ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        // bucketNumber can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public ListenableFuture<List<ConnectorSplit>> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, List<ConnectorSplit>>> function)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void noMoreSplits()
                    {
                        queue.finish();
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.isFinished();
                    }

                    @Override
                    public int rewind(OptionalInt bucketNumber)
                    {
                        throw new UnsupportedOperationException("rewind is not supported for non bucketed split source");
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                highMemorySplitSourceCounter,
                false);
    }

    public static HiveSplitSource bucketed(
            ConnectorSession session,
            String databaseName,
            String tableName,
            CacheQuotaRequirement cacheQuotaRequirement,
            int maxInitialSplits,
            int estimatedOutstandingSplitsPerBucket,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                cacheQuotaRequirement,
                new PerBucket()
                {
                    private final Map<Integer, AsyncQueue<InternalHiveSplit>> queues = new ConcurrentHashMap<>();
                    private final AtomicBoolean finished = new AtomicBoolean();

                    @Override
                    public ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        AsyncQueue<InternalHiveSplit> queue = queueFor(bucketNumber);
                        queue.offer(connectorSplit);
                        // Do not block "offer" when running split discovery in bucketed mode.
                        // A limit is enforced on estimatedSplitSizeInBytes.
                        return immediateFuture(null);
                    }

                    @Override
                    public ListenableFuture<List<ConnectorSplit>> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, List<ConnectorSplit>>> function)
                    {
                        return queueFor(bucketNumber).borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void noMoreSplits()
                    {
                        if (finished.compareAndSet(false, true)) {
                            queues.values().forEach(AsyncQueue::finish);
                        }
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        return queueFor(bucketNumber).isFinished();
                    }

                    @Override
                    public int rewind(OptionalInt bucketNumber)
                    {
                        throw new UnsupportedOperationException("rewind is not supported for unrewindable split source");
                    }

                    private AsyncQueue<InternalHiveSplit> queueFor(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent());
                        AtomicBoolean isNew = new AtomicBoolean();
                        AsyncQueue<InternalHiveSplit> queue = queues.computeIfAbsent(bucketNumber.getAsInt(), ignored -> {
                            isNew.set(true);
                            return new AsyncQueue<>(estimatedOutstandingSplitsPerBucket, executor);
                        });
                        if (isNew.get() && finished.get()) {
                            // Check `finished` and invoke `queue.finish` after the `queue` is added to the map.
                            // Otherwise, `queue.finish` may not be invoked if `finished` is set while the lambda above is being evaluated.
                            queue.finish();
                        }
                        return queue;
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                highMemorySplitSourceCounter,
                false);
    }

    public static HiveSplitSource bucketedRewindable(
            ConnectorSession session,
            String databaseName,
            String tableName,
            CacheQuotaRequirement cacheQuotaRequirement,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                cacheQuotaRequirement,
                new PerBucket()
                {
                    @GuardedBy("this")
                    private final Map<Integer, List<InternalHiveSplit>> splits = new HashMap<>();
                    private final SettableFuture<?> allSplitLoaded = SettableFuture.create();

                    @Override
                    public synchronized ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        checkArgument(bucketNumber.isPresent(), "bucketNumber must be present");
                        splits.computeIfAbsent(bucketNumber.getAsInt(), ignored -> new ArrayList<>()).add(connectorSplit);
                        // Do not block "offer" when running split discovery in bucketed mode.
                        return immediateFuture(null);
                    }

                    @Override
                    public synchronized ListenableFuture<List<ConnectorSplit>> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, List<ConnectorSplit>>> function)
                    {
                        checkArgument(bucketNumber.isPresent(), "bucketNumber must be present");
                        if (!allSplitLoaded.isDone()) {
                            return allSplitLoaded.transform(ignored -> ImmutableList.of(), executor);
                        }
                        return immediateFuture(function.apply(getSplits(bucketNumber.getAsInt(), maxSize)).getResult());
                    }

                    private List<InternalHiveSplit> getSplits(int bucketNumber, int batchSize)
                    {
                        return splits.getOrDefault(bucketNumber, ImmutableList.of()).stream()
                                .filter(split -> !split.isDone())
                                .limit(batchSize)
                                .collect(toImmutableList());
                    }

                    @Override
                    public void noMoreSplits()
                    {
                        allSplitLoaded.set(null);
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent(), "bucketNumber must be present");
                        // For virtual buckets, not all the buckets would exist, so we need to handle non-existent bucket.
                        return allSplitLoaded.isDone() && (!splits.containsKey(bucketNumber.getAsInt()) || splits.get(bucketNumber.getAsInt()).stream().allMatch(InternalHiveSplit::isDone));
                    }

                    @Override
                    public synchronized int rewind(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent(), "bucketNumber must be present");
                        checkState(allSplitLoaded.isDone(), "splits cannot be rewound before splits enumeration is finished");
                        int revivedSplitCount = 0;
                        for (InternalHiveSplit split : splits.getOrDefault(bucketNumber.getAsInt(), ImmutableList.of())) {
                            if (split.isDone()) {
                                revivedSplitCount++;
                            }
                            split.reset();
                        }
                        return revivedSplitCount;
                    }

                    @Override
                    public int decrementAndGetPartitionReferences(InternalHiveSplit split)
                    {
                        // we keep all splits forever so references should never decrease
                        throw new UnsupportedOperationException("decrementPartitionReferences is not supported for rewindable split sources");
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                highMemorySplitSourceCounter,
                true);
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

    ListenableFuture<?> addToQueue(List<? extends InternalHiveSplit> splits)
    {
        ListenableFuture<?> lastResult = immediateFuture(null);
        for (InternalHiveSplit split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    ListenableFuture<?> addToQueue(InternalHiveSplit split)
    {
        if (stateReference.get().getKind() != INITIAL) {
            return immediateFuture(null);
        }

        // The PartitionInfo isn't included in the size of the InternalHiveSplit
        // because it's a shared object. If this is the first InternalHiveSplit
        // for that PartitionInfo, add its cost
        if (split.getPartitionInfo().incrementAndGetReferences() == 1) {
            estimatedSplitSizeInBytes.addAndGet(split.getPartitionInfo().getEstimatedSizeInBytes());
        }

        if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
            // TODO: investigate alternative split discovery strategies when this error is hit.
            // This limit should never be hit given there is a limit of maxOutstandingSplits.
            // If it's hit, it means individual splits are huge.
            if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                highMemorySplitSourceCounter.update(1);
                log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
            }
            throw new PrestoException(HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT, format(
                    "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                    databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
        }
        bufferedInternalSplitCount.incrementAndGet();
        OptionalInt bucketNumber = split.getReadBucketNumber();
        return queues.offer(bucketNumber, split);
    }

    void noMoreSplits()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.noMoreSplits();
        }
    }

    void fail(Throwable e)
    {
        // The error must be recorded before setting the noMoreSplits marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.noMoreSplits();
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        boolean noMoreSplits;
        State state = stateReference.get();
        switch (state.getKind()) {
            case INITIAL:
                noMoreSplits = false;
                break;
            case NO_MORE_SPLITS:
                noMoreSplits = true;
                break;
            case FAILED:
                return failedFuture(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }

        OptionalInt bucketNumber = toBucketNumber(partitionHandle);
        ListenableFuture<List<ConnectorSplit>> future = queues.borrowBatchAsync(bucketNumber, maxSize, internalSplits -> {
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
                    long remainingBlockBytes = block.getEnd() - internalSplit.getStart();
                    if (remainingBlockBytes <= maxSplitBytes) {
                        splitBytes = remainingBlockBytes;
                    }
                    else if (maxSplitBytes * 2 >= remainingBlockBytes) {
                        //  Second to last split in this block, generate two evenly sized splits
                        splitBytes = remainingBlockBytes / 2;
                    }
                    else {
                        splitBytes = maxSplitBytes;
                    }
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
                        internalSplit.getFileModifiedTime(),
                        internalSplit.getPartitionInfo().getStorage(),
                        internalSplit.getPartitionKeys(),
                        block.getAddresses(),
                        internalSplit.getReadBucketNumber(),
                        internalSplit.getTableBucketNumber(),
                        internalSplit.getNodeSelectionStrategy(),
                        internalSplit.getPartitionInfo().getPartitionDataColumnCount(),
                        internalSplit.getTableToPartitionMapping(),
                        internalSplit.getBucketConversion(),
                        internalSplit.isS3SelectPushdownEnabled(),
                        internalSplit.getExtraFileInfo(),
                        cacheQuotaRequirement,
                        internalSplit.getEncryptionInformation(),
                        internalSplit.getCustomSplitInfo(),
                        internalSplit.getPartitionInfo().getRedundantColumnDomains()));

                internalSplit.increaseStart(splitBytes);

                if (internalSplit.isDone()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();

                    // rewindable split sources keep their splits forever
                    if (!useRewindableSplitSource && queues.decrementAndGetPartitionReferences(internalSplit) == 0) {
                        removedEstimatedSizeInBytes += internalSplit.getPartitionInfo().getEstimatedSizeInBytes();
                    }
                }
                else {
                    splitsToInsertBuilder.add(internalSplit);
                }
            }

            // For rewindable split source, we keep all the splits in memory.
            if (!useRewindableSplitSource) {
                estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);
            }

            List<InternalHiveSplit> splitsToInsert = splitsToInsertBuilder.build();
            List<ConnectorSplit> result = resultBuilder.build();
            bufferedInternalSplitCount.addAndGet(splitsToInsert.size() - result.size());

            return new AsyncQueue.BorrowResult<>(splitsToInsert, result);
        });

        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, splits -> {
            requireNonNull(splits, "splits is null");
            if (noMoreSplits) {
                // Checking splits.isEmpty() here is required for thread safety.
                // Let's say there are 10 splits left, and max number of splits per batch is 5.
                // The futures constructed in two getNextBatch calls could each fetch 5, resulting in zero splits left.
                // After fetching the splits, both futures reach this line at the same time.
                // Without the isEmpty check, both will claim they are the last.
                // Side note 1: In such a case, it doesn't actually matter which one gets to claim it's the last.
                //              But having both claim they are the last would be a surprising behavior.
                // Side note 2: One could argue that the isEmpty check is overly conservative.
                //              The caller of getNextBatch will likely need to make an extra invocation.
                //              But an extra invocation likely doesn't matter.
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queues.isFinished(bucketNumber));
            }
            else {
                return new ConnectorSplitBatch(splits, false);
            }
        }, directExecutor());

        return toCompletableFuture(transform);
    }

    @Override
    public void rewind(ConnectorPartitionHandle partitionHandle)
    {
        bufferedInternalSplitCount.addAndGet(queues.rewind(toBucketNumber(partitionHandle)));
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        switch (state.getKind()) {
            case INITIAL:
                return false;
            case NO_MORE_SPLITS:
                return bufferedInternalSplitCount.get() == 0;
            case FAILED:
                throw propagatePrestoException(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == INITIAL || state.getKind() == NO_MORE_SPLITS)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.noMoreSplits();
        }
    }

    private static OptionalInt toBucketNumber(ConnectorPartitionHandle partitionHandle)
    {
        if (partitionHandle == NOT_PARTITIONED) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(((HivePartitionHandle) partitionHandle).getBucket());
    }

    private static <T> boolean setIf(AtomicReference<T> atomicReference, T newValue, Predicate<T> predicate)
    {
        while (true) {
            T current = atomicReference.get();
            if (!predicate.test(current)) {
                return false;
            }
            if (atomicReference.compareAndSet(current, newValue)) {
                return true;
            }
        }
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

    interface PerBucket
    {
        ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit split);

        ListenableFuture<List<ConnectorSplit>> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, List<ConnectorSplit>>> function);

        void noMoreSplits();

        boolean isFinished(OptionalInt bucketNumber);

        // returns the number of finished InternalHiveSplits that are rewound
        int rewind(OptionalInt bucketNumber);

        default int decrementAndGetPartitionReferences(InternalHiveSplit split)
        {
            return split.getPartitionInfo().decrementAndGetReferences();
        }
    }

    static class State
    {
        private final StateKind kind;
        private final Throwable throwable;

        private State(StateKind kind, Throwable throwable)
        {
            this.kind = kind;
            this.throwable = throwable;
        }

        public StateKind getKind()
        {
            return kind;
        }

        public Throwable getThrowable()
        {
            checkState(throwable != null);
            return throwable;
        }

        public static State initial()
        {
            return new State(INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(FAILED, throwable);
        }

        public static State closed()
        {
            return new State(CLOSED, null);
        }
    }

    enum StateKind
    {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }
}
