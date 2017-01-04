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
import com.facebook.presto.execution.buffer.ClientBuffer.PageReference;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class BroadcastOutputBuffer
        implements OutputBuffer
{
    private final String taskInstanceId;
    private final StateMachine<BufferState> state;
    private final OutputBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = OutputBuffers.createInitialEmptyOutputBuffers(BROADCAST);

    @GuardedBy("this")
    private final Map<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final List<PageReference> initialPagesForNewBuffers = new ArrayList<>();

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();
    private final AtomicLong totalBufferedPages = new AtomicLong();

    public BroadcastOutputBuffer(
            String taskInstanceId,
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            SystemMemoryUsageListener systemMemoryUsageListener,
            Executor notificationExecutor)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");
        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
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

        // always get the state first before any other stats
        BufferState state = this.state.get();

        // buffer it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        return new OutputBufferInfo(
                "BROADCAST",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                buffers.stream()
                        .map(ClientBuffer::getInfo)
                        .collect(toImmutableList()));
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
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
            for (Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
                if (!buffers.containsKey(entry.getKey())) {
                    ClientBuffer buffer = getBuffer(entry.getKey());
                    if (!state.canAddPages()) {
                        buffer.setNoMorePages();
                    }
                }
            }

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
    public ListenableFuture<?> enqueue(Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        // split the page
        List<Page> pages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // reserve memory
        long bytesAdded = pages.stream().mapToLong(Page::getRetainedSizeInBytes).sum();
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = pages.stream().mapToLong(Page::getPositionCount).sum();
        checkState(rowCount == page.getPositionCount());
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());
        totalBufferedPages.addAndGet(pages.size());

        // create page reference counts with an initial single reference
        List<PageReference> pageReferences = pages.stream()
                .map(pageSplit -> new PageReference(pageSplit, 1, () -> {
                    checkState(totalBufferedPages.decrementAndGet() >= 0);
                    memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes());
                }))
                .collect(toImmutableList());

        // if we can still add buffers, remember the pages for the future buffers
        Collection<ClientBuffer> buffers;
        synchronized (this) {
            if (state.get().canAddBuffers()) {
                pageReferences.forEach(ClientBuffer.PageReference::addReference);
                initialPagesForNewBuffers.addAll(pageReferences);
            }

            // make a copy while holding the lock to avoid race with initialPagesForNewBuffers.addAll above
            buffers = safeGetBuffersSnapshot();
        }

        // add pages to all existing buffers (each buffer will increment the reference count)
        buffers.forEach(partition -> partition.enqueuePages(pageReferences));

        // drop the initial reference
        pageReferences.forEach(ClientBuffer.PageReference::dereferencePage);

        return memoryManager.getNotFullFuture();
    }

    @Override
    public ListenableFuture<?> enqueue(int partitionNumber, Page page)
    {
        checkState(partitionNumber == 0, "Expected partition number to be zero");
        return enqueue(page);
    }

    @Override
    public CompletableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(outputBufferId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        safeGetBuffersSnapshot().forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

            memoryManager.setNoBlockOnFull();
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            memoryManager.setNoBlockOnFull();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    private synchronized ClientBuffer getBuffer(OutputBufferId id)
    {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        checkState(state.get().canAddBuffers(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id);

        // add initial pages
        buffer.enqueuePages(initialPagesForNewBuffers);

        // update state
        if (!state.get().canAddPages()) {
            buffer.setNoMorePages();
        }

        // buffer may have finished immediately before calling this method
        if (state.get() == FINISHED) {
            buffer.destroy();
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private void noMoreBuffers()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more buffers while holding a lock on this");
        List<PageReference> pages;
        synchronized (this) {
            pages = ImmutableList.copyOf(initialPagesForNewBuffers);
            initialPagesForNewBuffers.clear();

            // verify all created buffers have been declared
            SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
            checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        pages.forEach(ClientBuffer.PageReference::dereferencePage);
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING) {
            return;
        }

        for (ClientBuffer buffer : safeGetBuffersSnapshot()) {
            if (!buffer.isDestroyed()) {
                return;
            }
        }
        destroy();
    }
}
