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
package com.facebook.presto.execution.buffer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PageDataOutput;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.SPOOLING;
import static com.facebook.presto.spi.StandardErrorCode.SPOOLING_STORAGE_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.collect.Range.closedOpen;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnstableApiUsage")
public class SpoolingOutputBuffer
        implements OutputBuffer
{
    private final TaskId taskId;
    private final String taskInstanceId;
    private final OutputBuffers outputBuffers;
    private final StateMachine<BufferState> state;
    private final TempDataOperationContext tempDataOperationContext;
    private final TempStorage tempStorage;
    private final DataSize threshold;
    private final FinalizerService finalizerService;
    private final ListeningExecutorService executor;

    private final AtomicLong totalBufferedBytes = new AtomicLong();
    private final AtomicLong totalBufferedPages = new AtomicLong();
    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    private final OutputBufferId outputBufferId = new OutputBufferId(0);

    private static final Logger log = Logger.get(SpoolingOutputBuffer.class);

    private final AtomicBoolean noMorePages = new AtomicBoolean();
    private final AtomicLong currentMemorySequenceId = new AtomicLong();
    private final AtomicLong currentSequenceId = new AtomicLong();
    private final AtomicLong startPage = new AtomicLong();
    private final AtomicLong totalPagesRemaining = new AtomicLong();
    private final AtomicLong totalInMemoryBytes = new AtomicLong();
    private final AtomicLong peakMemoryUsage = new AtomicLong();

    private final AtomicLong totalStorageBytesAdded = new AtomicLong();
    private final AtomicLong totalStoragePagesAdded = new AtomicLong();

    @GuardedBy("this")
    private final Deque<HandleInfo> handleInfoQueue = new LinkedList<>();

    @GuardedBy("this")
    private final Queue<SerializedPage> pages = new ArrayDeque<>();

    @GuardedBy("this")
    private PendingRead pendingRead;

    public SpoolingOutputBuffer(
            TaskId taskId,
            String taskInstanceId,
            OutputBuffers outputBuffers,
            StateMachine<BufferState> state,
            TempStorage tempStorage,
            DataSize threshold,
            ListeningExecutorService executor,
            FinalizerService finalizerService)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceIs is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.state = requireNonNull(state, "state is null");
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.threshold = requireNonNull(threshold, "threshold is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        this.finalizerService.addFinalizer(this, this::close);

        tempDataOperationContext = new TempDataOperationContext(Optional.empty(), taskId.getQueryId().toString(), Optional.empty(), Optional.empty(), new Identity("spooling-buffer", Optional.empty()));

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        return new OutputBufferInfo(
                "SPOOLING",
                state.get(),
                state.get().canAddBuffers(),
                state.get().canAddPages(),
                totalBufferedBytes.get(),
                totalBufferedPages.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                ImmutableList.of());
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == BufferState.FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return totalInMemoryBytes.get() / (double) threshold.toBytes();
    }

    @Override
    public boolean isOverutilized()
    {
        return totalInMemoryBytes.get() > threshold.toBytes();
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return immediateFuture(null);
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");
        checkArgument(outputBuffers.getType() == SPOOLING, "Invalid output buffers type");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "invalid noMoreBufferIds");

        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        if (!state.get().canAddPages()) {
            return;
        }
        PendingRead pendingRead;
        synchronized (this) {
            this.pages.addAll(pages);

            long bytesAdded = getPagesSize(pages);
            long pagesAdded = pages.size();

            // update output buffer info
            totalBufferedBytes.addAndGet(bytesAdded);
            totalBufferedPages.addAndGet(pagesAdded);
            totalPagesAdded.addAndGet(pagesAdded);
            totalRowsAdded.addAndGet(getPagesRows(pages));

            totalInMemoryBytes.addAndGet(bytesAdded);

            totalPagesRemaining.addAndGet(pagesAdded);
            peakMemoryUsage.accumulateAndGet(totalInMemoryBytes.get(), Math::max);

            if (totalInMemoryBytes.get() >= threshold.toBytes()) {
                flush();
            }

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        if (pendingRead != null) {
            processPendingRead(pendingRead);
        }
    }

    @Override
    public synchronized void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        checkState(partition == 0, "Expected partition number to be zero");
        enqueue(lifespan, pages);
    }

    private synchronized void flush()
    {
        List<DataOutput> dataOutputs = pages.stream()
                .map(PageDataOutput::new)
                .collect(toImmutableList());

        // create a future that will hold the handle
        ListenableFuture<TempStorageHandle> handleFuture = executor.submit(() -> {
            TempDataSink dataSink = tempStorage.create(tempDataOperationContext);
            dataSink.write(dataOutputs);
            return dataSink.commit();
        });

        // store the handleFuture and file information
        long bytes = totalInMemoryBytes.get();
        int pageCount = pages.size();
        HandleInfo handleInfo = new HandleInfo(
                closedOpen(currentMemorySequenceId.get(), currentMemorySequenceId.get() + pageCount),
                handleFuture,
                bytes,
                pageCount);
        handleInfoQueue.add(handleInfo);

        // update cutoff for file pages
        currentMemorySequenceId.addAndGet(pageCount);

        // clear the pages in memory
        pages.clear();

        // update info about storage
        totalStorageBytesAdded.addAndGet(bytes);
        totalStoragePagesAdded.addAndGet(pageCount);
        totalInMemoryBytes.set(0);
    }

    @Override
    public synchronized ListenableFuture<BufferResult> get(OutputBufferId bufferId, long startSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "outputBufferId is null");
        checkArgument(bufferId.getId() == outputBufferId.getId(), "Invalid buffer id");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        acknowledge(bufferId, startSequenceId);

        long currentSequenceId = this.currentSequenceId.get();

        // process the request if we have no more data coming in, have data to read, or if this is an outdated request
        if (noMorePages.get() || !handleInfoQueue.isEmpty() || !pages.isEmpty() || currentSequenceId != startSequenceId) {
            return processRead(startSequenceId, maxSize);
        }

        // creating a pending read, and abort the previous one
        PendingRead oldPendingRead = pendingRead;
        pendingRead = new PendingRead(taskInstanceId, currentSequenceId, maxSize);

        if (oldPendingRead != null) {
            oldPendingRead.completeResultFutureWithEmpty();
        }

        return pendingRead.getResultFuture();
    }

    private void processPendingRead(PendingRead pendingRead)
    {
        if (pendingRead.getResultFuture().isDone()) {
            return;
        }

        ListenableFuture<BufferResult> resultFuture = processRead(pendingRead.getStartSequenceId(), pendingRead.getMaxSize());
        pendingRead.setResultFuture(resultFuture);
    }

    private synchronized ListenableFuture<BufferResult> processRead(long startSequenceId, DataSize maxSize)
    {
        long currentSequenceId = this.currentSequenceId.get();

        // startSequenceId is for a page before the current page position
        if (startSequenceId < currentSequenceId) {
            return immediateFuture(emptyResults(taskInstanceId, startSequenceId, false));
        }

        // tells client that buffer is complete
        if (noMorePages.get() && handleInfoQueue.isEmpty() && pages.isEmpty()) {
            return immediateFuture(emptyResults(taskInstanceId, startSequenceId, true));
        }

        // validate previous pages were acknowledged
        checkState(currentSequenceId == startSequenceId, "Invalid startSequenceId");

        // get a copy of in memory pages and the HandleInfo to save the handleFuture
        List<HandleInfo> handleInfos = ImmutableList.copyOf(handleInfoQueue);
        List<SerializedPage> pages = ImmutableList.copyOf(this.pages);

        GetTracker getTracker = new GetTracker(maxSize, handleInfos, pages, toIntExact(startPage.get()));
        long maxBytes = maxSize.toBytes();

        // read pages
        ListenableFuture<List<SerializedPage>> storagePages = getPagesFromStorage(startSequenceId, getTracker);
        ListenableFuture<List<SerializedPage>> memoryPages = transform(storagePages, input -> {
            long pageCount = getTracker.getPageCount();
            long bytes = getTracker.getBytes();
            long startMemorySequenceId = startSequenceId + pageCount;
            if (startMemorySequenceId == currentMemorySequenceId.get() && (bytes < maxBytes || input.isEmpty())) {
                ImmutableList.Builder<SerializedPage> combinedPages = ImmutableList.builder();
                combinedPages.addAll(input);
                combinedPages.addAll(getPagesFromMemory(startMemorySequenceId, getTracker));
                return combinedPages.build();
            }
            return input;
        }, executor);

        ListenableFuture<BufferResult> resultFuture = transform(memoryPages, input -> {
            long newSequenceId = startSequenceId + input.size();
            return new BufferResult(taskInstanceId, startSequenceId, newSequenceId, false, input);
        }, executor);

        return catchingAsync(resultFuture, Exception.class, e -> {
            log.error("Task %s: Failed to get page with startSequenceId %s", taskId, startSequenceId);
            return immediateFailedFuture(e);
        }, executor);
    }

    private ListenableFuture<List<SerializedPage>> getPagesFromStorage(long startSequenceId, GetTracker getTracker)
    {
        if (startSequenceId >= currentMemorySequenceId.get()) {
            return immediateFuture(ImmutableList.of());
        }

        Iterator<HandleInfo> handleInfoIterator = getTracker.getHandleInfos().iterator();
        HandleInfo handleInfo = handleInfoIterator.next();
        ListenableFuture<TempStorageHandle> handleFuture = handleInfo.getHandleFuture();
        return transformAsync(handleFuture, input -> getPagesFromStorage(ImmutableList.builder(), handleInfoIterator, input, getTracker), executor);
    }

    private ListenableFuture<List<SerializedPage>> getPagesFromStorage(ImmutableList.Builder<SerializedPage> resultBuilder, Iterator<HandleInfo> handleIterator, TempStorageHandle handle, GetTracker getTracker)
    {
        long maxBytes = getTracker.getMaxSize().toBytes();
        long bytes = getTracker.getBytes();
        long pageCount = getTracker.getPageCount();

        try (SliceInput inputStream = new InputStreamSliceInput(tempStorage.open(tempDataOperationContext, handle))) {
            Iterator<SerializedPage> serializedPages = readSerializedPages(inputStream);
            advance(serializedPages, getTracker.getStartPage());

            while (serializedPages.hasNext()) {
                SerializedPage page = serializedPages.next();
                long bytesRead = bytes;
                bytes += page.getRetainedSizeInBytes();

                if (pageCount != 0 && bytes > maxBytes) {
                    getTracker.update(bytesRead, pageCount);
                    return immediateFuture(resultBuilder.build());
                }
                resultBuilder.add(page);
                pageCount++;
            }

            getTracker.update(bytes, pageCount);

            if (!handleIterator.hasNext()) {
                return immediateFuture(resultBuilder.build());
            }
            return transformAsync(handleIterator.next().getHandleFuture(), input -> getPagesFromStorage(resultBuilder, handleIterator, input, getTracker), executor);
        }
        catch (IOException e) {
            throw new PrestoException(SPOOLING_STORAGE_ERROR, "Failed to read file from TempStorage", e);
        }
    }

    private List<SerializedPage> getPagesFromMemory(long startSequenceId, GetTracker getTracker)
    {
        checkArgument(startSequenceId == currentMemorySequenceId.get(), "Invalid startSequenceId for memory pages");
        checkArgument(getTracker.bytes < getTracker.maxSize.toBytes(), "bytesRead is greater than maxSize");

        ImmutableList.Builder<SerializedPage> result = ImmutableList.builder();
        List<SerializedPage> pages = getTracker.getMemoryPages();
        long maxBytes = getTracker.maxSize.toBytes();
        long bytes = 0;
        long pageCount = 0;

        for (SerializedPage page : pages) {
            bytes += page.getRetainedSizeInBytes();
            if (pageCount != 0 && bytes > maxBytes) {
                break;
            }
            result.add(page);
            pageCount++;
        }

        return result.build();
    }

    @Override
    public synchronized void acknowledge(OutputBufferId bufferId, long sequenceId)
    {
        checkArgument(bufferId.getId() == outputBufferId.getId(), "Invalid buffer id");
        checkArgument(sequenceId >= 0, "Invalid sequenceId");

        // ignore if buffer is destroyed OR pages have been acknowledged already
        if (state.get() == FINISHED || sequenceId < currentSequenceId.get()) {
            return;
        }

        long oldSequenceId = currentSequenceId.get();
        int pagesToRemove = toIntExact(sequenceId - oldSequenceId);
        long currentSequenceId = oldSequenceId;

        checkArgument(pagesToRemove <= totalPagesRemaining.get(), "Invalid sequenceId");

        // remove the pages from storage
        currentSequenceId += acknowledgePagesFromStorage(sequenceId);

        // remove the pages from memory
        if (currentSequenceId < sequenceId) {
            acknowledgePagesFromMemory(sequenceId, currentSequenceId);
        }

        verify(this.currentSequenceId.compareAndSet(oldSequenceId, oldSequenceId + pagesToRemove));
    }

    private synchronized long acknowledgePagesFromStorage(long sequenceId)
    {
        long pagesAcknowledged = 0;
        long pagesRemoved = 0;
        long bytesRemoved = 0;
        List<HandleInfo> handleInfos = ImmutableList.copyOf(handleInfoQueue);

        for (HandleInfo handleInfo : handleInfos) {
            Range<Long> range = handleInfo.getRange();

            if (range.upperEndpoint() <= sequenceId) {
                handleInfo.removeFile();
                handleInfoQueue.removeFirst();

                pagesAcknowledged += handleInfo.getPageCount() - startPage.get();
                pagesRemoved += handleInfo.getPageCount();
                bytesRemoved += handleInfo.getBytes();

                startPage.set(0);
            }
            else {
                pagesAcknowledged += sequenceId - range.lowerEndpoint() - startPage.get();
                startPage.set(toIntExact(sequenceId - range.lowerEndpoint()));
                break;
            }
        }

        totalBufferedPages.addAndGet(-pagesRemoved);
        totalBufferedBytes.addAndGet(-bytesRemoved);
        totalPagesRemaining.addAndGet(-pagesAcknowledged);

        return pagesAcknowledged;
    }

    private synchronized void acknowledgePagesFromMemory(long sequenceId, long startSequenceId)
    {
        checkState(startSequenceId == currentMemorySequenceId.get(), "Invalid startSequenceId for memory pages");
        int pagesToRemove = toIntExact(sequenceId - startSequenceId);
        checkArgument(pagesToRemove <= pages.size(), "Invalid sequenceId");

        long bytesRemoved = 0;
        for (int i = 0; i < pagesToRemove; i++) {
            SerializedPage removedPage = pages.remove();
            bytesRemoved += removedPage.getRetainedSizeInBytes();
            currentMemorySequenceId.incrementAndGet();
        }

        totalBufferedPages.addAndGet(-pagesToRemove);
        totalBufferedBytes.addAndGet(-bytesRemoved);
        totalInMemoryBytes.addAndGet(-bytesRemoved);
        totalPagesRemaining.addAndGet(-pagesToRemove);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        checkArgument(bufferId.getId() == outputBufferId.getId(), "Invalid bufferId");
        destroy();
    }

    @Override
    public void setNoMorePages()
    {
        PendingRead pendingRead;
        synchronized (this) {
            state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
            noMorePages.set(true);

            pendingRead = this.pendingRead;
            this.pendingRead = null;

            log.info("Task %s: %s pages and %s bytes was written into TempStorage", taskId, totalStoragePagesAdded.get(), totalStorageBytesAdded.get());
        }

        if (pendingRead != null) {
            processPendingRead(pendingRead);
        }

        checkFlushComplete();
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING) {
            return;
        }

        if (totalBufferedPages.get() == 0) {
            destroy();
        }
    }

    @Override
    public void destroy()
    {
        PendingRead pendingRead;
        synchronized (this) {
            if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
                close();
            }
            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        if (pendingRead != null) {
            pendingRead.completeResultFutureWithEmpty();
        }
    }

    @Override
    public void fail()
    {
        state.setIf(BufferState.FAILED, oldState -> !oldState.isTerminal());
    }

    private synchronized void close()
    {
        for (HandleInfo handleInfo : handleInfoQueue) {
            handleInfo.removeFile();
        }
        pages.clear();
        handleInfoQueue.clear();
        noMorePages.set(true);
        totalBufferedPages.set(0);
        totalBufferedBytes.set(0);
        totalPagesRemaining.set(0);
    }

    @Override
    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        // NOOP
    }

    @Override
    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        // NOOP
    }

    @Override
    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        return isFinished();
    }

    private long getPagesSize(Collection<SerializedPage> pages)
    {
        return pages.stream().mapToLong(SerializedPage::getRetainedSizeInBytes).sum();
    }

    private long getPagesRows(Collection<SerializedPage> pages)
    {
        return pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
    }

    private class HandleInfo
    {
        private final Range<Long> range;
        private final ListenableFuture<TempStorageHandle> handleFuture;
        private final long bytes;
        private final int pageCount;

        public HandleInfo(Range<Long> range, ListenableFuture<TempStorageHandle> handleFuture, long bytes, int pageCount)
        {
            this.range = requireNonNull(range, "range is null");
            this.handleFuture = requireNonNull(handleFuture, "handleFuture is null");
            this.bytes = bytes;
            this.pageCount = pageCount;
        }

        public long getBytes()
        {
            return bytes;
        }

        public int getPageCount()
        {
            return pageCount;
        }

        public Range<Long> getRange()
        {
            return range;
        }

        public ListenableFuture<TempStorageHandle> getHandleFuture()
        {
            return handleFuture;
        }

        public void removeFile()
        {
            executor.execute(() -> {
                try {
                    tempStorage.remove(tempDataOperationContext, handleFuture.get());
                }
                catch (Exception e) {
                    log.error(e, "Failed to remove file from TempStorage");
                }
            });
        }
    }

    @Immutable
    private static class PendingRead
    {
        private final String taskInstanceId;
        private final long startSequenceId;
        private final DataSize maxSize;
        private final SettableFuture<BufferResult> resultFuture = SettableFuture.create();

        private PendingRead(String taskInstanceId, long startSequenceId, DataSize maxSize)
        {
            this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
            this.startSequenceId = startSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public long getStartSequenceId()
        {
            return startSequenceId;
        }

        public DataSize getMaxSize()
        {
            return maxSize;
        }

        public ListenableFuture<BufferResult> getResultFuture()
        {
            return resultFuture;
        }

        public void completeResultFutureWithEmpty()
        {
            resultFuture.set(emptyResults(taskInstanceId, startSequenceId, false));
        }

        public void setResultFuture(ListenableFuture<BufferResult> result)
        {
            resultFuture.setFuture(result);
        }
    }

    private class GetTracker
    {
        private int startPage;
        private long bytes;
        private long pageCount;

        private final DataSize maxSize;
        private final List<SerializedPage> pages;
        private final List<HandleInfo> handleInfos;

        private GetTracker(DataSize maxSize, List<HandleInfo> handleInfos, List<SerializedPage> pages, int startPage)
        {
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
            this.handleInfos = requireNonNull(handleInfos, "handleInfos is null");
            this.pages = requireNonNull(pages, "pages is null");
            this.startPage = startPage;
        }

        private void update(long newBytes, long newPageCount)
        {
            bytes = newBytes;
            pageCount = newPageCount;
            startPage = 0;
        }

        private DataSize getMaxSize()
        {
            return maxSize;
        }

        private int getStartPage()
        {
            return startPage;
        }

        private long getBytes()
        {
            return bytes;
        }

        private long getPageCount()
        {
            return pageCount;
        }

        private List<SerializedPage> getMemoryPages()
        {
            return pages;
        }

        private List<HandleInfo> getHandleInfos()
        {
            return handleInfos;
        }
    }
}
