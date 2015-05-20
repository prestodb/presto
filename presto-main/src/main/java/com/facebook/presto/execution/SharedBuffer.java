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

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.execution.BufferResult.emptyResults;
import static com.facebook.presto.execution.SharedBuffer.BufferState.FAILED;
import static com.facebook.presto.execution.SharedBuffer.BufferState.FINISHED;
import static com.facebook.presto.execution.SharedBuffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.SharedBuffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.SharedBuffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.SharedBuffer.BufferState.OPEN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SharedBuffer
{
    public enum BufferState
    {
        /**
         * Additional buffers can be added.
         * Any next state is allowed.
         */
        OPEN(true, true, false),
        /**
         * No more buffers can be added.
         * Next state is {@link #FLUSHING}.
         */
        NO_MORE_BUFFERS(true, false, false),
        /**
         * No more pages can be added.
         * Next state is {@link #FLUSHING}.
         */
        NO_MORE_PAGES(false, true, false),
        /**
         * No more pages or buffers can be added, and buffer is waiting
         * for the final pages to be consumed.
         * Next state is {@link #FINISHED}.
         */
        FLUSHING(false, false, false),
        /**
         * No more buffers can be added and all pages have been consumed.
         * This is the terminal state.
         */
        FINISHED(false, false, true),
        /**
         * Buffer has failed.  No more buffers or pages can be added.  Readers
         * will be blocked, as to not communicate a finished state.  It is
         * assumed that the reader will be cleaned up elsewhere.
         * This is the terminal state.
         */
        FAILED(false, false, true);

        private final boolean newPagesAllowed;
        private final boolean newBuffersAllowed;
        private final boolean terminal;

        BufferState(boolean newPagesAllowed, boolean newBuffersAllowed, boolean terminal)
        {
            this.newPagesAllowed = newPagesAllowed;
            this.newBuffersAllowed = newBuffersAllowed;
            this.terminal = terminal;
        }

        public boolean canAddPages()
        {
            return newPagesAllowed;
        }

        public boolean canAddBuffers()
        {
            return newBuffersAllowed;
        }

        public boolean isTerminal()
        {
            return terminal;
        }
    }

    private final SettableFuture<OutputBuffers> finalOutputBuffers = SettableFuture.create();

    @GuardedBy("this")
    private OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;
    @GuardedBy("this")
    private final Map<Integer, PartitionBuffer> partitionBuffers = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Map<Integer, Set<NamedBuffer>> partitionToNamedBuffer = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final ConcurrentMap<TaskId, NamedBuffer> namedBuffers = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<TaskId> abortedBuffers = new HashSet<>();

    private final StateMachine<BufferState> state;

    @GuardedBy("this")
    private final List<GetBufferResult> stateChangeListeners = new ArrayList<>();

    private final SharedBufferMemoryManager memoryManager;

    public SharedBuffer(TaskId taskId, Executor executor, DataSize maxBufferSize)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(executor, "executor is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN);

        checkNotNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.memoryManager = new SharedBufferMemoryManager(maxBufferSize.toBytes());
    }

    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    public SharedBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free to we are not hanging state machine updates
        //
        checkState(!Thread.holdsLock(this), "Thread must NOT hold a lock on the %s", SharedBuffer.class.getSimpleName());
        BufferState state = this.state.get();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (NamedBuffer namedBuffer : namedBuffers.values()) {
            infos.add(namedBuffer.getInfo());
        }

        long totalBufferedBytes = partitionBuffers.values().stream().mapToLong(PartitionBuffer::getBufferedBytes).sum();
        long totalBufferedPages = partitionBuffers.values().stream().mapToLong(PartitionBuffer::getBufferedPageCount).sum();
        long totalQueuedPages = partitionBuffers.values().stream().mapToLong(PartitionBuffer::getQueuedPageCount).sum();
        long totalPagesSent = partitionBuffers.values().stream().mapToLong(PartitionBuffer::getPageCount).sum();

        return new SharedBufferInfo(state, state.canAddBuffers(), state.canAddPages(), totalBufferedBytes, totalBufferedPages, totalQueuedPages, totalPagesSent, infos.build());
    }

    public ListenableFuture<OutputBuffers> getFinalOutputBuffers()
    {
        return finalOutputBuffers;
    }

    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkNotNull(newOutputBuffers, "newOutputBuffers is null");
        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // verify this is valid state change
        SetView<TaskId> missingBuffers = Sets.difference(outputBuffers.getBuffers().keySet(), newOutputBuffers.getBuffers().keySet());
        checkArgument(missingBuffers.isEmpty(), "newOutputBuffers does not have existing buffers %s", missingBuffers);
        checkArgument(!outputBuffers.isNoMoreBufferIds() || newOutputBuffers.isNoMoreBufferIds(), "Expected newOutputBuffers to have noMoreBufferIds set");
        outputBuffers = newOutputBuffers;

        // add the new buffers
        for (Entry<TaskId, PagePartitionFunction> entry : outputBuffers.getBuffers().entrySet()) {
            TaskId bufferId = entry.getKey();
            if (!namedBuffers.containsKey(bufferId)) {
                checkState(state.get().canAddBuffers(), "Cannot add buffers to %s", SharedBuffer.class.getSimpleName());
                PagePartitionFunction partitionFunction = entry.getValue();

                int partition = 0;
                if (partitionFunction instanceof HashPagePartitionFunction) {
                    partition = ((HashPagePartitionFunction) partitionFunction).getPartition();
                }

                PartitionBuffer partitionBuffer = createOrGetPartitionBuffer(partition);
                NamedBuffer namedBuffer = new NamedBuffer(bufferId, partitionBuffer);

                // the buffer may have been aborted before the creation message was received
                if (abortedBuffers.contains(bufferId)) {
                    namedBuffer.abort();
                }
                namedBuffers.put(bufferId, namedBuffer);
                Set<NamedBuffer> namedBuffers = partitionToNamedBuffer.computeIfAbsent(partition, k -> new HashSet<>());
                namedBuffers.add(namedBuffer);
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

    private PartitionBuffer createOrGetPartitionBuffer(int partition)
    {
        checkState(Thread.holdsLock(this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());
        return partitionBuffers.computeIfAbsent(partition, k -> new PartitionBuffer(partition, memoryManager));
    }

    public synchronized ListenableFuture<?> enqueue(Page page)
    {
        return enqueue(0, page);
    }

    public synchronized ListenableFuture<?> enqueue(int partition, Page page)
    {
        checkNotNull(page, "page is null");

        // ignore pages after no more pages is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        PartitionBuffer partitionBuffer = createOrGetPartitionBuffer(partition);
        ListenableFuture<?> result = partitionBuffer.enqueuePage(page);
        processPendingReads();
        updateState();
        return result;
    }

    public synchronized ListenableFuture<BufferResult> get(TaskId outputId, long startingSequenceId, DataSize maxSize)
    {
        checkNotNull(outputId, "outputId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        // if no buffers can be added, and the requested buffer does not exist, return a closed empty result
        // this can happen with limit queries
        BufferState state = this.state.get();
        if (state != FAILED && !state.canAddBuffers() && namedBuffers.get(outputId) == null) {
            return immediateFuture(emptyResults(0, true));
        }

        // return a future for data
        GetBufferResult getBufferResult = new GetBufferResult(outputId, startingSequenceId, maxSize);
        stateChangeListeners.add(getBufferResult);
        updateState();
        return getBufferResult.getFuture();
    }

    public synchronized void abort(TaskId outputId)
    {
        checkNotNull(outputId, "outputId is null");

        abortedBuffers.add(outputId);

        NamedBuffer namedBuffer = namedBuffers.get(outputId);
        if (namedBuffer != null) {
            namedBuffer.abort();
        }

        updateState();
    }

    public synchronized void setNoMorePages()
    {
        if (state.compareAndSet(OPEN, NO_MORE_PAGES) || state.compareAndSet(NO_MORE_BUFFERS, FLUSHING)) {
            updateState();
        }
    }

    /**
     * Destroys the buffer, discarding all pages.
     */
    public synchronized void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.get().isTerminal()) {
            return;
        }

        state.set(FINISHED);

        partitionBuffers.values().forEach(PartitionBuffer::destroy);
        // free readers
        namedBuffers.values().forEach(SharedBuffer.NamedBuffer::abort);
        processPendingReads();
    }

    /**
     * Fail the buffer, discarding all pages, but blocking readers.
     */
    public synchronized void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.get().isTerminal()) {
            return;
        }

        state.set(FAILED);
        partitionBuffers.values().forEach(PartitionBuffer::destroy);

        // DO NOT free readers
    }

    private void checkFlushComplete()
    {
        checkState(Thread.holdsLock(this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

        if (state.get() == FLUSHING) {
            for (NamedBuffer namedBuffer : namedBuffers.values()) {
                if (!namedBuffer.checkCompletion()) {
                    return;
                }
            }
            destroy();
        }
    }

    private void updateState()
    {
        checkState(Thread.holdsLock(this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

        try {
            processPendingReads();

            BufferState state = this.state.get();

            // do not update if the buffer is already in a terminal state
            if (state.isTerminal()) {
                return;
            }

            if (!state.canAddPages()) {
                // discard queued pages (not officially in the buffer)
                partitionBuffers.values().forEach(PartitionBuffer::clearQueue);
            }

            // advanced master queue
            if (!state.canAddBuffers() && !namedBuffers.isEmpty()) {
                for (Map.Entry<Integer, Set<NamedBuffer>> entry : partitionToNamedBuffer.entrySet()) {
                    PartitionBuffer partitionBuffer = partitionBuffers.get(entry.getKey());
                    long newMasterSequenceId = entry.getValue().stream()
                            .mapToLong(NamedBuffer::getSequenceId)
                            .min()
                            .getAsLong();
                    partitionBuffers.get(entry.getKey()).advanceSequenceId(newMasterSequenceId);
                    partitionBuffer.advanceSequenceId(newMasterSequenceId);
                }
            }

            // remove any completed buffers
            if (!state.canAddPages()) {
                namedBuffers.values().forEach(SharedBuffer.NamedBuffer::checkCompletion);
            }
        }
        finally {
            checkFlushComplete();
        }
    }

    private void processPendingReads()
    {
        checkState(Thread.holdsLock(this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

        ImmutableList.copyOf(stateChangeListeners).stream().filter(GetBufferResult::execute).forEach(stateChangeListeners::remove);
    }

    @ThreadSafe
    private final class NamedBuffer
    {
        private final TaskId bufferId;
        private final PartitionBuffer partitionBuffer;

        private final AtomicLong sequenceId = new AtomicLong();
        private final AtomicBoolean finished = new AtomicBoolean();

        private NamedBuffer(TaskId bufferId, PartitionBuffer partitionBuffer)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.partitionBuffer = requireNonNull(partitionBuffer, "partitionBuffer is null");
        }

        public BufferInfo getInfo()
        {
            //
            // NOTE: this code must be lock free to we are not hanging state machine updates
            //
            checkState(!Thread.holdsLock(SharedBuffer.this), "Thread must NOT hold a lock on the %s", SharedBuffer.class.getSimpleName());

            long sequenceId = this.sequenceId.get();

            if (finished.get()) {
                return new BufferInfo(bufferId, true, 0, sequenceId, partitionBuffer.getInfo());
            }

            int bufferedPages = Math.max(Ints.checkedCast(partitionBuffer.getPageCount() - sequenceId), 0);
            return new BufferInfo(bufferId, finished.get(), bufferedPages, sequenceId, partitionBuffer.getInfo());
        }

        public long getSequenceId()
        {
            checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            return sequenceId.get();
        }

        public BufferResult getPages(long startingSequenceId, DataSize maxSize)
        {
            checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());
            checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

            long sequenceId = this.sequenceId.get();
            checkArgument(startingSequenceId >= sequenceId, "startingSequenceId is before the beginning of the buffer");

            // acknowledge previous pages
            if (startingSequenceId > sequenceId) {
                this.sequenceId.set(startingSequenceId);
                sequenceId = startingSequenceId;
            }

            if (checkCompletion()) {
                return emptyResults(startingSequenceId, true);
            }

            List<Page> pages = partitionBuffer.getPages(maxSize, sequenceId);
            return new BufferResult(startingSequenceId, startingSequenceId + pages.size(), false, pages);
        }

        public void abort()
        {
            checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            finished.set(true);
        }

        public boolean checkCompletion()
        {
            checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());
            // WARNING: finish must short circuit this call, or the call to checkFlushComplete below will cause an infinite recursion
            if (finished.get()) {
                return true;
            }

            long pagesAdded = partitionBuffer.getPageCount();
            if (!state.get().canAddPages() && sequenceId.get() >= pagesAdded) {
                // WARNING: finish must set before the call to checkFlushComplete of the short circuit above will not trigger and the code enter an infinite recursion
                finished.set(true);

                // check if master buffer is finished
                checkFlushComplete();
            }
            return finished.get();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferId", bufferId)
                    .add("sequenceId", sequenceId.get())
                    .add("finished", finished.get())
                    .toString();
        }
    }

    @Immutable
    private class GetBufferResult
    {
        private final SettableFuture<BufferResult> future = SettableFuture.create();

        private final TaskId outputId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        public GetBufferResult(TaskId outputId, long startingSequenceId, DataSize maxSize)
        {
            this.outputId = outputId;
            this.startingSequenceId = startingSequenceId;
            this.maxSize = maxSize;
        }

        public SettableFuture<BufferResult> getFuture()
        {
            return future;
        }

        public boolean execute()
        {
            checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

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
                    future.set(emptyResults(namedBuffer == null ? 0 : namedBuffer.getSequenceId(), true));
                    return true;
                }

                // buffer doesn't exist yet. Block reader until buffer is created
                if (namedBuffer == null) {
                    return false;
                }

                // if request is for pages before the current position, just return an empty page
                if (startingSequenceId < namedBuffer.getSequenceId()) {
                    future.set(emptyResults(startingSequenceId, false));
                    return true;
                }

                // read pages from the buffer
                BufferResult bufferResult = namedBuffer.getPages(startingSequenceId, maxSize);

                // if this was the last page, we're done
                checkFlushComplete();

                // if we got an empty result, wait for more pages
                if (bufferResult.isEmpty() && !bufferResult.isBufferClosed()) {
                    return false;
                }

                future.set(bufferResult);
            }
            catch (Throwable throwable) {
                future.setException(throwable);
            }
            return true;
        }
    }
}
