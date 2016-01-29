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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * A buffer that assigns pages to queus base on a first come, first served basis.
 */
public class ArbitraryOutputBuffer
        implements OutputBuffer
{
    private final SettableFuture<OutputBuffers> finalOutputBuffers = SettableFuture.create();
    private final OutputBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
    @GuardedBy("this")
    private final MasterBuffer masterBuffer;
    @GuardedBy("this")
    private final ConcurrentMap<OutputBufferId, NamedBuffer> namedBuffers = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    private final StateMachine<BufferState> state;
    private final String taskInstanceId;

    @GuardedBy("this")
    private final List<GetBufferResult> stateChangeListeners = new ArrayList<>();

    public ArbitraryOutputBuffer(String taskInstanceId, StateMachine<BufferState> state, DataSize maxBufferSize, SystemMemoryUsageListener systemMemoryUsageListener, Executor notificationExecutor)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.masterBuffer = new MasterBuffer(memoryManager);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //
        checkDoesNotHoldLock();
        BufferState state = this.state.get();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (NamedBuffer namedBuffer : namedBuffers.values()) {
            infos.add(namedBuffer.getInfo());
        }

        return new OutputBufferInfo(
                "ARBITRARY",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                masterBuffer.getBufferedBytes(),
                masterBuffer.getBufferedPageCount(),
                masterBuffer.getRowCount(),
                masterBuffer.getPageCount(),
                infos.build());
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
        outputBuffers = newOutputBuffers;

        // add the new buffers
        for (Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
            OutputBufferId bufferId = entry.getKey();
            if (!namedBuffers.containsKey(bufferId)) {
                checkState(state.get().canAddBuffers(), "Cannot add buffers to %s", getClass().getSimpleName());

                NamedBuffer namedBuffer = new NamedBuffer(bufferId, masterBuffer);

                // the buffer may have been aborted before the creation message was received
                if (abortedBuffers.contains(bufferId)) {
                    namedBuffer.abort();
                }
                namedBuffers.put(bufferId, namedBuffer);
            }
        }

        // update state if no more buffers is set
        if (outputBuffers.isNoMoreBufferIds()) {
            state.compareAndSet(OPEN, NO_MORE_BUFFERS);
            state.compareAndSet(NO_MORE_PAGES, FLUSHING);
            finalOutputBuffers.set(outputBuffers);
        }

        updateState();
    }

    @Override
    public synchronized ListenableFuture<?> enqueue(Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after no more pages is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        ListenableFuture<?> result = masterBuffer.enqueuePage(page);
        processPendingReads();
        updateState();
        return result;
    }

    @Override
    public synchronized ListenableFuture<?> enqueue(int partition, Page page)
    {
        throw new UnsupportedOperationException("ArbitraryOutputBuffer does not support partitions");
    }

    @Override
    public synchronized CompletableFuture<BufferResult> get(OutputBufferId outputId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputId, "outputId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        // if no buffers can be added, and the requested buffer does not exist, return a closed empty result
        // this can happen with limit queries
        BufferState state = this.state.get();
        if (state != FAILED && !state.canAddBuffers() && namedBuffers.get(outputId) == null) {
            return completedFuture(emptyResults(taskInstanceId, 0, true));
        }

        // return a future for data
        GetBufferResult getBufferResult = new GetBufferResult(outputId, startingSequenceId, maxSize);
        stateChangeListeners.add(getBufferResult);
        updateState();
        return getBufferResult.getFuture();
    }

    @Override
    public synchronized void abort(OutputBufferId outputId)
    {
        requireNonNull(outputId, "outputId is null");

        abortedBuffers.add(outputId);

        NamedBuffer namedBuffer = namedBuffers.get(outputId);
        if (namedBuffer != null) {
            namedBuffer.abort();
        }

        updateState();
    }

    @Override
    public synchronized void setNoMorePages()
    {
        if (state.compareAndSet(OPEN, NO_MORE_PAGES) || state.compareAndSet(NO_MORE_BUFFERS, FLUSHING)) {
            updateState();
        }
    }

    @Override
    public synchronized void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.get().isTerminal()) {
            return;
        }

        state.set(FINISHED);

        masterBuffer.destroy();
        // free readers
        namedBuffers.values().forEach(NamedBuffer::abort);
        processPendingReads();
    }

    @Override
    public synchronized void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.get().isTerminal()) {
            return;
        }

        state.set(FAILED);
        masterBuffer.destroy();

        // DO NOT free readers
    }

    private void checkFlushComplete()
    {
        checkHoldsLock();

        if (state.get() == FLUSHING) {
            for (NamedBuffer namedBuffer : namedBuffers.values()) {
                if (!namedBuffer.isFinished()) {
                    return;
                }
            }
            destroy();
        }
    }

    private void updateState()
    {
        checkHoldsLock();

        try {
            processPendingReads();

            BufferState state = this.state.get();

            // do not update if the buffer is already in a terminal state
            if (state.isTerminal()) {
                return;
            }
        }
        finally {
            checkFlushComplete();
        }
    }

    private void processPendingReads()
    {
        checkHoldsLock();
        Set<GetBufferResult> finishedListeners = ImmutableList.copyOf(stateChangeListeners).stream().filter(GetBufferResult::execute).collect(toImmutableSet());
        stateChangeListeners.removeAll(finishedListeners);
    }

    private void checkHoldsLock()
    {
        // This intentionally does not use checkState, because it's called *very* frequently. To the point that
        // SharedBuffer.class.getSimpleName() showed up in perf
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    private void checkDoesNotHoldLock()
    {
        if (Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must NOT hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    @Immutable
    private class GetBufferResult
    {
        private final CompletableFuture<BufferResult> future = new CompletableFuture<>();

        private final OutputBufferId outputId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        public GetBufferResult(OutputBufferId outputId, long startingSequenceId, DataSize maxSize)
        {
            this.outputId = outputId;
            this.startingSequenceId = startingSequenceId;
            this.maxSize = maxSize;
        }

        public CompletableFuture<BufferResult> getFuture()
        {
            return future;
        }

        public boolean execute()
        {
            checkHoldsLock();

            if (future.isDone()) {
                return true;
            }

            // Buffer is failed, block the reader.  Eventually, the reader will be aborted by the coordinator.
            if (state.get() == FAILED) {
                return false;
            }

            try {
                NamedBuffer namedBuffer = namedBuffers.get(outputId);

                // if buffer is finished return an empty page
                // this could be a request for a buffer that never existed, but that is ok since the buffer
                // could have been destroyed before the creation message was received
                if (state.get() == FINISHED) {
                    future.complete(emptyResults(taskInstanceId, namedBuffer == null ? 0 : namedBuffer.getCurrentToken(), true));
                    return true;
                }

                // buffer doesn't exist yet. Block reader until buffer is created
                if (namedBuffer == null) {
                    return false;
                }

                // if request is for pages before the current position, just return an empty page
                if (startingSequenceId < namedBuffer.getCurrentToken() - 1) {
                    future.complete(emptyResults(taskInstanceId, startingSequenceId, false));
                    return true;
                }

                // read pages from the buffer
                Optional<BufferResult> bufferResult = namedBuffer.getPages(startingSequenceId, maxSize);

                // if this was the last page, we're done
                checkFlushComplete();

                // if we got an empty result, wait for more pages
                if (!bufferResult.isPresent()) {
                    return false;
                }

                future.complete(bufferResult.get());
            }
            catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
            return true;
        }
    }

    @ThreadSafe
    private final class NamedBuffer
    {
        private final OutputBufferId bufferId;
        private final MasterBuffer masterBuffer;

        private final AtomicLong currentToken = new AtomicLong();
        private final AtomicBoolean finished = new AtomicBoolean();

        private List<Page> lastPages = ImmutableList.of();

        private NamedBuffer(OutputBufferId bufferId, MasterBuffer masterBuffer)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.masterBuffer = requireNonNull(masterBuffer, "partitionBuffer is null");
        }

        public BufferInfo getInfo()
        {
            //
            // NOTE: this code must be lock free to we are not hanging state machine updates
            //
            checkDoesNotHoldLock();

            long sequenceId = this.currentToken.get();

            if (finished.get()) {
                return new BufferInfo(bufferId, true, 0, sequenceId, masterBuffer.getInfo());
            }

            int bufferedPages = Math.max(toIntExact(masterBuffer.getPageCount() - sequenceId), 0);
            return new BufferInfo(bufferId, finished.get(), bufferedPages, sequenceId, masterBuffer.getInfo());
        }

        public long getCurrentToken()
        {
            checkHoldsLock();

            return currentToken.get();
        }

        public Optional<BufferResult> getPages(long token, DataSize maxSize)
        {
            checkHoldsLock();
            checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

            // is this a request for the previous pages
            if (token == currentToken.get() - 1) {
                return Optional.of(new BufferResult(taskInstanceId, token, token + 1, false, lastPages));
            }

            checkArgument(token == currentToken.get(), "Invalid token");

            if (isFinished()) {
                lastPages = ImmutableList.of();
                return Optional.of(emptyResults(taskInstanceId, token, true));
            }

            List<Page> pages = masterBuffer.removePages(maxSize);

            if (pages.isEmpty()) {
                if (state.get().canAddPages()) {
                    // since there is no data, don't move the counter forward since this would require a client round trip
                    return Optional.empty();
                }

                // if we can't have any more pages, indicate that the buffer is complete
                lastPages = ImmutableList.of();
                return Optional.of(emptyResults(taskInstanceId, token, true));
            }

            // we have data, move the counter forward
            currentToken.incrementAndGet();
            lastPages = pages;
            return Optional.of(new BufferResult(taskInstanceId, token, token + 1, false, lastPages));
        }

        public void abort()
        {
            checkHoldsLock();

            finished.set(true);
            checkFlushComplete();
        }

        public boolean isFinished()
        {
            checkHoldsLock();
            return finished.get();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferId", bufferId)
                    .add("sequenceId", currentToken.get())
                    .add("finished", finished.get())
                    .toString();
        }
    }

    private static class MasterBuffer
    {
        private final LinkedList<Page> masterBuffer = new LinkedList<>();

        // Total number of pages added to this buffer
        private final AtomicLong pagesAdded = new AtomicLong();
        private final AtomicLong rowsAdded = new AtomicLong();

        // Bytes in this buffer
        private final AtomicLong bufferedBytes = new AtomicLong();

        private final OutputBufferMemoryManager memoryManager;

        public MasterBuffer(OutputBufferMemoryManager memoryManager)
        {
            this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        }

        public synchronized ListenableFuture<?> enqueuePage(Page page)
        {
            addToMasterBuffer(page);
            return memoryManager.getNotFullFuture();
        }

        private synchronized void addToMasterBuffer(Page page)
        {
            List<Page> pages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
            masterBuffer.addAll(pages);

            long rowCount = pages.stream().mapToLong(Page::getPositionCount).sum();
            checkState(rowCount == page.getPositionCount());
            rowsAdded.addAndGet(rowCount);
            pagesAdded.addAndGet(pages.size());

            updateMemoryUsage(pages.stream().mapToLong(Page::getSizeInBytes).sum());
        }

        /**
         * @return at least one page if we have pages in buffer, empty list otherwise
         */
        public synchronized List<Page> removePages(DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<Page> pages = new ArrayList<>();
            long bytesRemoved = 0;

            while (true) {
                Page page = masterBuffer.peek();
                if (page == null) {
                    break;
                }
                bytesRemoved += page.getSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                    break;
                }
                // this should not happen since we have a lock
                checkState(masterBuffer.poll() == page, "Master buffer corrupted");
                pages.add(page);
            }

            updateMemoryUsage(-bytesRemoved);

            return ImmutableList.copyOf(pages);
        }

        public synchronized void destroy()
        {
            // clear the buffer
            masterBuffer.clear();
            updateMemoryUsage(-bufferedBytes.get());
        }

        private void updateMemoryUsage(long bytesAdded)
        {
            bufferedBytes.addAndGet(bytesAdded);
            memoryManager.updateMemoryUsage(bytesAdded);
            verify(bufferedBytes.get() >= 0);
        }

        public long getRowCount()
        {
            return rowsAdded.get();
        }

        public long getPageCount()
        {
            return pagesAdded.get();
        }

        public long getBufferedBytes()
        {
            return bufferedBytes.get();
        }

        public long getBufferedPageCount()
        {
            return masterBuffer.size();
        }

        public PageBufferInfo getInfo()
        {
            return new PageBufferInfo(0, getBufferedPageCount(), getBufferedBytes(), rowsAdded.get(), pagesAdded.get());
        }
    }
}
