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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.ClientBuffer.PagesSupplier;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A buffer that assigns pages to queues based on a first come, first served basis.
 */
public class ArbitraryOutputBuffer
        implements OutputBuffer
{
    private final OutputBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);

    private final MasterBuffer masterBuffer;

    @GuardedBy("this")
    private final ConcurrentMap<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    //  The index of the first client buffer that should be polled
    private final AtomicInteger nextClientBufferIndex = new AtomicInteger(0);

    private final StateMachine<BufferState> state;
    private final String taskInstanceId;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    private final ConcurrentMap<Lifespan, AtomicLong> outstandingPageCountPerLifespan = new ConcurrentHashMap<>();
    private final Set<Lifespan> noMorePagesForLifespan = ConcurrentHashMap.newKeySet();
    private volatile Consumer<Lifespan> lifespanCompletionCallback;

    public ArbitraryOutputBuffer(
            String taskInstanceId,
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.masterBuffer = new MasterBuffer();
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
    public boolean isOverutilized()
    {
        return (memoryManager.getUtilization() >= 0.5) || !state.get().canAddPages();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        // buffers it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        int totalBufferedPages = masterBuffer.getBufferedPages();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer buffer : buffers) {
            BufferInfo bufferInfo = buffer.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferInfo(
                "ARBITRARY",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkState(!Thread.holdsLock(this), "Can not set output buffers while holding a lock on this");
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState state = this.state.get();
            if (state.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = newOutputBuffers;

            // add the new buffers
            for (OutputBufferId outputBufferId : outputBuffers.getBuffers().keySet()) {
                getBuffer(outputBufferId);
            }
            // Reset resume from position
            nextClientBufferIndex.set(0);

            // update state if no more buffers is set
            if (outputBuffers.isNoMoreBufferIds()) {
                this.state.compareAndSet(OPEN, NO_MORE_BUFFERS);
                this.state.compareAndSet(NO_MORE_PAGES, FLUSHING);
            }
        }

        if (!state.get().canAddBuffers()) {
            noMoreBuffers();
        }

        checkFlushComplete();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        checkState(lifespanCompletionCallback == null, "lifespanCompletionCallback is already set");
        this.lifespanCompletionCallback = requireNonNull(callback, "callback is null");
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        checkState(!Thread.holdsLock(this), "Can not enqueue pages while holding a lock on this");
        requireNonNull(lifespan, "lifespan is null");
        requireNonNull(pages, "page is null");
        checkState(lifespanCompletionCallback != null, "lifespanCompletionCallback must be set before enqueueing data");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages() || noMorePagesForLifespan.contains(lifespan)) {
            return;
        }

        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long bytesAdded = 0;
        long rowCount = 0;
        for (SerializedPage page : pages) {
            long retainedSize = page.getRetainedSizeInBytes();
            bytesAdded += retainedSize;
            rowCount += page.getPositionCount();
            // create page reference counts with an initial single reference
            references.add(new SerializedPageReference(page, 1, () -> dereferencePage(page, lifespan)));
        }
        List<SerializedPageReference> serializedPageReferences = references.build();

        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(serializedPageReferences.size());
        outstandingPageCountPerLifespan.computeIfAbsent(lifespan, ignored -> new AtomicLong()).addAndGet(serializedPageReferences.size());

        // add pages to the buffer (this will increase the reference count by one)
        masterBuffer.addPages(serializedPageReferences);

        // process any pending reads from the client buffers
        List<ClientBuffer> buffers = safeGetBuffersSnapshot();
        if (buffers.isEmpty()) {
            return;
        }
        // handle potential for racy update of next index and client buffers present
        int index = nextClientBufferIndex.get() % buffers.size();
        for (int i = 0; i < buffers.size(); i++) {
            if (masterBuffer.isEmpty()) {
                // Resume from the current client buffer on the next iteration
                nextClientBufferIndex.set(index);
                break;
            }
            buffers.get(index).loadPagesIfNecessary(masterBuffer);
            index = (index + 1) % buffers.size();
        }
    }

    @Override
    public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        checkState(partition == 0, "Expected partition number to be zero");
        enqueue(lifespan, pages);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not get pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(bufferId).getPages(startingSequenceId, maxSize, Optional.of(masterBuffer));
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Can not acknowledge pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        checkState(!Thread.holdsLock(this), "Can not abort while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more pages while holding a lock on this");
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        masterBuffer.setNoMorePages();

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Can not destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            masterBuffer.destroy();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    @Override
    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        requireNonNull(lifespan, "lifespan is null");
        noMorePagesForLifespan.add(lifespan);
    }

    @Override
    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        if (!noMorePagesForLifespan.contains(lifespan)) {
            return false;
        }

        AtomicLong outstandingPageCount = outstandingPageCountPerLifespan.get(lifespan);
        return outstandingPageCount == null || outstandingPageCount.get() == 0;
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    private synchronized ClientBuffer getBuffer(OutputBufferId id)
    {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        // NOTE: buffers are allowed to be created in the FINISHED state because destroy() can move to the finished state
        // without a clean "no-more-buffers" message from the scheduler.  This happens with limit queries and is ok because
        // the buffer will be immediately destroyed.
        checkState(state.get().canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id);

        // buffer may have finished immediately before calling this method
        if (state.get() == FINISHED) {
            buffer.destroy();
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized List<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private synchronized void noMoreBuffers()
    {
        if (outputBuffers.isNoMoreBufferIds()) {
            // verify all created buffers have been declared
            SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
            checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
        }
    }

    @GuardedBy("this")
    private void checkFlushComplete()
    {
        // This buffer type assigns each page to a single, arbitrary reader,
        // so we don't need to wait for no-more-buffers to finish the buffer.
        // Any readers added after finish will simply receive no data.
        BufferState state = this.state.get();
        if ((state == FLUSHING) || ((state == NO_MORE_PAGES) && masterBuffer.isEmpty())) {
            if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
                destroy();
            }
        }
    }

    @ThreadSafe
    private static class MasterBuffer
            implements PagesSupplier
    {
        @GuardedBy("this")
        private final LinkedList<SerializedPageReference> masterBuffer = new LinkedList<>();

        @GuardedBy("this")
        private boolean noMorePages;

        private final AtomicInteger bufferedPages = new AtomicInteger();

        public synchronized void addPages(List<SerializedPageReference> pages)
        {
            masterBuffer.addAll(pages);
            bufferedPages.set(masterBuffer.size());
        }

        public synchronized boolean isEmpty()
        {
            return masterBuffer.isEmpty();
        }

        @Override
        public synchronized boolean mayHaveMorePages()
        {
            return !noMorePages || !masterBuffer.isEmpty();
        }

        public synchronized void setNoMorePages()
        {
            this.noMorePages = true;
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<SerializedPageReference> pages = new ArrayList<>();
            long bytesRemoved = 0;

            while (true) {
                SerializedPageReference page = masterBuffer.peek();
                if (page == null) {
                    break;
                }
                bytesRemoved += page.getRetainedSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                    break;
                }
                // this should not happen since we have a lock
                checkState(masterBuffer.poll() == page, "Master buffer corrupted");
                pages.add(page);
            }

            bufferedPages.set(masterBuffer.size());

            return ImmutableList.copyOf(pages);
        }

        public void destroy()
        {
            checkState(!Thread.holdsLock(this), "Can not destroy master buffer while holding a lock on this");
            List<SerializedPageReference> pages;
            synchronized (this) {
                pages = ImmutableList.copyOf(masterBuffer);
                masterBuffer.clear();
                bufferedPages.set(0);
            }

            // dereference outside of synchronized to avoid making a callback while holding a lock
            pages.forEach(SerializedPageReference::dereferencePage);
        }

        public int getBufferedPages()
        {
            return bufferedPages.get();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferedPages", bufferedPages.get())
                    .toString();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }

    private void dereferencePage(SerializedPage pageSplit, Lifespan lifespan)
    {
        long outstandingPageCount = outstandingPageCountPerLifespan.get(lifespan).decrementAndGet();
        if (outstandingPageCount == 0 && noMorePagesForLifespan.contains(lifespan)) {
            checkState(lifespanCompletionCallback != null, "lifespanCompletionCallback is not null");
            lifespanCompletionCallback.accept(lifespan);
        }

        memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes());
    }
}
