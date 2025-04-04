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
package com.facebook.presto.operator.index;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INDEX_LOADER_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class IndexLoader
{
    private static final ConnectorId INDEX_CONNECTOR_ID = new ConnectorId("$index");
    private final BlockingQueue<UpdateRequest> updateRequests = new LinkedBlockingQueue<>();

    private final List<Type> outputTypes;
    private final IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider;
    private final int expectedPositions;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats stats;

    private final AtomicReference<TaskContext> taskContextReference = new AtomicReference<>();
    private final Set<Integer> lookupSourceInputChannels;
    private final List<Integer> keyOutputChannels;
    private final OptionalInt keyOutputHashChannel;
    private final List<Type> keyTypes;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final Duration indexLoaderTimeout;

    @GuardedBy("this")
    private IndexSnapshotLoader indexSnapshotLoader; // Lazily initialized

    @GuardedBy("this")
    private PipelineContext pipelineContext; // Lazily initialized

    @GuardedBy("this")
    private final AtomicReference<IndexSnapshot> indexSnapshotReference;

    public IndexLoader(
            Set<Integer> lookupSourceInputChannels,
            List<Integer> keyOutputChannels,
            OptionalInt keyOutputHashChannel,
            List<Type> outputTypes,
            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
            int expectedPositions,
            DataSize maxIndexMemorySize,
            IndexJoinLookupStats stats,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            Duration indexLoaderTimeout)
    {
        requireNonNull(lookupSourceInputChannels, "lookupSourceInputChannels is null");
        checkArgument(!lookupSourceInputChannels.isEmpty(), "lookupSourceInputChannels must not be empty");
        requireNonNull(keyOutputChannels, "keyOutputChannels is null");
        checkArgument(!keyOutputChannels.isEmpty(), "keyOutputChannels must not be empty");
        requireNonNull(keyOutputHashChannel, "keyOutputHashChannel is null");
        checkArgument(lookupSourceInputChannels.size() <= keyOutputChannels.size(), "Lookup channels must supply a subset of the actual index columns");
        requireNonNull(outputTypes, "outputTypes is null");
        requireNonNull(indexBuildDriverFactoryProvider, "indexBuildDriverFactoryProvider is null");
        requireNonNull(maxIndexMemorySize, "maxIndexMemorySize is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        requireNonNull(joinCompiler, "joinCompiler is null");
        requireNonNull(indexLoaderTimeout, "indexLoaderTimeout is null");

        this.lookupSourceInputChannels = ImmutableSet.copyOf(lookupSourceInputChannels);
        this.keyOutputChannels = ImmutableList.copyOf(keyOutputChannels);
        this.keyOutputHashChannel = keyOutputHashChannel;
        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.indexBuildDriverFactoryProvider = indexBuildDriverFactoryProvider;
        this.expectedPositions = expectedPositions;
        this.maxIndexMemorySize = maxIndexMemorySize;
        this.stats = stats;
        this.pagesIndexFactory = pagesIndexFactory;
        this.joinCompiler = joinCompiler;
        this.indexLoaderTimeout = indexLoaderTimeout;

        ImmutableList.Builder<Type> keyTypeBuilder = ImmutableList.builder();
        for (int keyOutputChannel : keyOutputChannels) {
            keyTypeBuilder.add(outputTypes.get(keyOutputChannel));
        }
        this.keyTypes = keyTypeBuilder.build();

        // start with an empty source
        this.indexSnapshotReference = new AtomicReference<>(new IndexSnapshot(new EmptyLookupSource(outputTypes.size()), new EmptyLookupSource(keyOutputChannels.size())));
    }

    // This is a ghetto way to acquire a TaskContext at runtime (unavailable at planning)
    public void setContext(TaskContext taskContext)
    {
        taskContextReference.compareAndSet(null, taskContext);
    }

    public int getChannelCount()
    {
        return outputTypes.size();
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public IndexSnapshot getIndexSnapshot()
    {
        return indexSnapshotReference.get();
    }

    public IndexedData getIndexedDataForKeys(int position, Page indexPage)
    {
        // Normalize the indexBlocks so that they only encompass the unloaded positions
        int totalPositions = indexPage.getPositionCount();
        int remainingPositions = totalPositions - position;
        return getIndexedDataForKeys(indexPage.getRegion(position, remainingPositions));
    }

    private IndexedData getIndexedDataForKeys(Page indexPage)
    {
        UpdateRequest myUpdateRequest = new UpdateRequest(indexPage);
        updateRequests.add(myUpdateRequest);

        synchronized (this) {
            if (!myUpdateRequest.isFinished()) {
                stats.recordIndexJoinLookup();
                initializeStateIfNecessary();

                List<UpdateRequest> requests = new ArrayList<>();
                updateRequests.drainTo(requests);

                try {
                    long initialCacheSizeInBytes = indexSnapshotLoader.getCacheSizeInBytes();

                    // TODO: add heuristic to jump to load strategy that is most likely to succeed

                    // Try to load all the requests
                    if (indexSnapshotLoader.load(requests)) {
                        return myUpdateRequest.getFinishedIndexSnapshot();
                    }

                    // Retry again if there was initial data (load failures will clear the cache automatically)
                    if (initialCacheSizeInBytes > 0 && indexSnapshotLoader.load(requests)) {
                        stats.recordSuccessfulIndexJoinLookupByCacheReset();
                        return myUpdateRequest.getFinishedIndexSnapshot();
                    }
                }
                catch (Throwable t) {
                    // Mark requests as failed since they will not be requeued
                    for (UpdateRequest request : requests) {
                        request.failed(t);
                    }
                    throw t;
                }

                // Try loading just my request
                if (requests.size() > 1) {
                    // Add all other requests back into the queue
                    Iterables.addAll(updateRequests, filter(requests, not(equalTo(myUpdateRequest))));

                    if (indexSnapshotLoader.load(ImmutableList.of(myUpdateRequest))) {
                        stats.recordSuccessfulIndexJoinLookupBySingleRequest();
                        return myUpdateRequest.getFinishedIndexSnapshot();
                    }
                }

                // Repeatedly decrease the number of rows to load by a factor of 10
                int totalPositions = indexPage.getPositionCount();
                int attemptedPositions = totalPositions / 10;
                while (attemptedPositions > 1) {
                    myUpdateRequest = new UpdateRequest(indexPage.getRegion(0, attemptedPositions));
                    if (indexSnapshotLoader.load(ImmutableList.of(myUpdateRequest))) {
                        stats.recordSuccessfulIndexJoinLookupByLimitedRequest();
                        return myUpdateRequest.getFinishedIndexSnapshot();
                    }
                    attemptedPositions /= 10;
                }

                // Just load the single index key in a streaming fashion (no caching)
                stats.recordStreamedIndexJoinLookup();
                return streamIndexDataForSingleKey(myUpdateRequest);
            }
        }

        // return the snapshot from the update request as another thread may have already flushed the request
        return myUpdateRequest.getFinishedIndexSnapshot();
    }

    public IndexedData streamIndexDataForSingleKey(UpdateRequest updateRequest)
    {
        Page indexKeyTuple = updateRequest.getPage().getRegion(0, 1);

        PageBuffer pageBuffer = new PageBuffer(100);
        DriverFactory driverFactory = indexBuildDriverFactoryProvider.createStreaming(pageBuffer, indexKeyTuple);
        Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext());

        PageRecordSet pageRecordSet = new PageRecordSet(keyTypes, indexKeyTuple);
        PlanNodeId planNodeId = driverFactory.getSourceId().get();
        ScheduledSplit split = new ScheduledSplit(0, planNodeId, new Split(INDEX_CONNECTOR_ID, new ConnectorTransactionHandle() {}, new IndexSplit(pageRecordSet)));
        driver.updateSource(new TaskSource(planNodeId, ImmutableSet.of(split), true));

        return new StreamingIndexedData(outputTypes, keyTypes, indexKeyTuple, pageBuffer, driver);
    }

    private synchronized void initializeStateIfNecessary()
    {
        if (pipelineContext == null) {
            TaskContext taskContext = taskContextReference.get();
            checkState(taskContext != null, "Task context must be set before index can be built");
            pipelineContext = taskContext.addPipelineContext(indexBuildDriverFactoryProvider.getPipelineId(), true, true, false);
        }
        if (indexSnapshotLoader == null) {
            indexSnapshotLoader = new IndexSnapshotLoader(
                    indexBuildDriverFactoryProvider,
                    pipelineContext,
                    indexSnapshotReference,
                    lookupSourceInputChannels,
                    keyTypes,
                    keyOutputChannels,
                    keyOutputHashChannel,
                    expectedPositions,
                    maxIndexMemorySize,
                    pagesIndexFactory,
                    joinCompiler,
                    indexLoaderTimeout);
        }
    }

    @NotThreadSafe
    private static class IndexSnapshotLoader
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;
        private final Set<Integer> lookupSourceInputChannels;
        private final Set<Integer> allInputChannels;
        private final List<Type> outputTypes;
        private final List<Type> indexTypes;
        private final AtomicReference<IndexSnapshot> indexSnapshotReference;
        private final JoinCompiler joinCompiler;

        private final IndexSnapshotBuilder indexSnapshotBuilder;
        private final Duration timeout;

        private IndexSnapshotLoader(IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
                PipelineContext pipelineContext,
                AtomicReference<IndexSnapshot> indexSnapshotReference,
                Set<Integer> lookupSourceInputChannels,
                List<Type> indexTypes,
                List<Integer> keyOutputChannels,
                OptionalInt keyOutputHashChannel,
                int expectedPositions,
                DataSize maxIndexMemorySize,
                PagesIndex.Factory pagesIndexFactory,
                JoinCompiler joinCompiler,
                Duration timeout)
        {
            this.pipelineContext = pipelineContext;
            this.indexSnapshotReference = indexSnapshotReference;
            this.lookupSourceInputChannels = lookupSourceInputChannels;
            this.outputTypes = indexBuildDriverFactoryProvider.getOutputTypes();
            this.indexTypes = indexTypes;
            this.joinCompiler = joinCompiler;
            this.timeout = timeout;

            this.indexSnapshotBuilder = new IndexSnapshotBuilder(
                    outputTypes,
                    keyOutputChannels,
                    keyOutputHashChannel,
                    pipelineContext.addDriverContext(),
                    maxIndexMemorySize,
                    expectedPositions,
                    pagesIndexFactory);
            this.driverFactory = indexBuildDriverFactoryProvider.createSnapshot(pipelineContext.getPipelineId(), this.indexSnapshotBuilder);

            ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
            for (int i = 0; i < indexTypes.size(); i++) {
                builder.add(i);
            }
            this.allInputChannels = builder.build();
        }

        public long getCacheSizeInBytes()
        {
            return indexSnapshotBuilder.getMemoryInBytes();
        }

        public boolean load(List<UpdateRequest> requests)
        {
            // Generate a RecordSet that only presents index keys that have not been cached and are deduped based on lookupSourceInputChannels
            UnloadedIndexKeyRecordSet recordSetForLookupSource = new UnloadedIndexKeyRecordSet(pipelineContext.getSession(), indexSnapshotReference.get(), lookupSourceInputChannels, indexTypes, requests, joinCompiler);

            // Drive index lookup to produce the output (landing in indexSnapshotBuilder)
            try (Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext())) {
                PlanNodeId sourcePlanNodeId = driverFactory.getSourceId().get();
                ScheduledSplit split = new ScheduledSplit(0, sourcePlanNodeId, new Split(INDEX_CONNECTOR_ID, new ConnectorTransactionHandle() {}, new IndexSplit(recordSetForLookupSource)));
                driver.updateSource(new TaskSource(sourcePlanNodeId, ImmutableSet.of(split), true));
                while (!driver.isFinished()) {
                    ListenableFuture<?> process = driver.process();
                    try {
                        process.get(timeout.toMillis(), MILLISECONDS);
                    }
                    catch (TimeoutException e) {
                        throw new PrestoException(INDEX_LOADER_TIMEOUT, format("Exceeded the time limit of %s loading indexes for index join", timeout));
                    }
                    catch (ExecutionException e) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error loading index for join", e);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error loading index for join", e);
                    }
                }
            }

            if (indexSnapshotBuilder.isMemoryExceeded()) {
                clearCachedData();
                return false;
            }

            // Generate a RecordSet that presents unique index keys that have not been cached
            UnloadedIndexKeyRecordSet indexKeysRecordSet = (lookupSourceInputChannels.equals(allInputChannels))
                    ? recordSetForLookupSource
                    : new UnloadedIndexKeyRecordSet(pipelineContext.getSession(), indexSnapshotReference.get(), allInputChannels, indexTypes, requests, joinCompiler);

            // Create lookup source with new data
            IndexSnapshot newValue = indexSnapshotBuilder.createIndexSnapshot(indexKeysRecordSet);
            if (newValue == null) {
                clearCachedData();
                return false;
            }

            indexSnapshotReference.set(newValue);
            for (UpdateRequest request : requests) {
                request.finished(newValue);
            }
            return true;
        }

        private void clearCachedData()
        {
            indexSnapshotReference.set(new IndexSnapshot(new EmptyLookupSource(outputTypes.size()), new EmptyLookupSource(indexTypes.size())));
            indexSnapshotBuilder.reset();
        }
    }

    private static class EmptyLookupSource
            implements LookupSource
    {
        private final int channelCount;

        public EmptyLookupSource(int channelCount)
        {
            this.channelCount = channelCount;
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public long getJoinPositionCount()
        {
            return 0;
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return 0;
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page page, Page allChannelsPage, long rawHash)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            return true;
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
