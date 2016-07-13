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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

@ThreadSafe
public class SharedOutputBuffer
        implements OutputBuffer
{
    private final SettableFuture<OutputBuffers> finalOutputBuffers = SettableFuture.create();

    @GuardedBy("this")
    private OutputBuffers outputBuffers;
    @GuardedBy("this")
    private final Map<Integer, SharedOutputBufferPartition> partitionBuffers = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Map<Integer, Set<NamedBuffer>> partitionToNamedBuffer = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final ConcurrentMap<OutputBufferId, NamedBuffer> namedBuffers = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    private final StateMachine<BufferState> state;
    private final String taskInstanceId;

    @GuardedBy("this")
    private final List<GetBufferResult> stateChangeListeners = new ArrayList<>();

    private final OutputBufferMemoryManager memoryManager;

    public SharedOutputBuffer(TaskId taskId, String taskInstanceId, Executor executor, DataSize maxBufferSize)
    {
        this(taskId, taskInstanceId, executor, maxBufferSize, deltaMemory -> { });
    }

    public SharedOutputBuffer(TaskId taskId, String taskInstanceId, Executor executor, DataSize maxBufferSize, SystemMemoryUsageListener systemMemoryUsageListener)
    {
        this(
                taskInstanceId,
                new StateMachine<>(
                        requireNonNull(taskId, "taskId is null") + "-buffer",
                        requireNonNull(executor, "executor is null"),
                        OPEN,
                        TERMINAL_BUFFER_STATES),
                maxBufferSize,
                systemMemoryUsageListener,
                executor);
    }

    public SharedOutputBuffer(String taskInstanceId,
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            SystemMemoryUsageListener systemMemoryUsageListener,
            Executor notificationExecutor)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null");
        requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.memoryManager = new OutputBufferMemoryManager(maxBufferSize.toBytes(), systemMemoryUsageListener, notificationExecutor);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free to we are not hanging state machine updates
        //
        checkDoesNotHoldLock();
        BufferState state = this.state.get();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (NamedBuffer namedBuffer : namedBuffers.values()) {
            infos.add(namedBuffer.getInfo());
        }

        long totalBufferedBytes = partitionBuffers.values().stream().mapToLong(SharedOutputBufferPartition::getBufferedBytes).sum();
        long totalBufferedPages = partitionBuffers.values().stream().mapToLong(SharedOutputBufferPartition::getBufferedPageCount).sum();
        long totalRowsSent = partitionBuffers.values().stream().mapToLong(SharedOutputBufferPartition::getRowCount).sum();
        long totalPagesSent = partitionBuffers.values().stream().mapToLong(SharedOutputBufferPartition::getPageCount).sum();

        return new OutputBufferInfo(
                "SHARED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                totalBufferedBytes,
                totalBufferedPages,
                totalRowsSent,
                totalPagesSent,
                infos.build());
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        if (outputBuffers == null) {
            outputBuffers = createInitialEmptyOutputBuffers(newOutputBuffers.getType());
        }

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

                int partition = entry.getValue();

                SharedOutputBufferPartition partitionBuffer = createOrGetPartitionBuffer(partition);
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

    private SharedOutputBufferPartition createOrGetPartitionBuffer(int partition)
    {
        checkHoldsLock();
        return partitionBuffers.computeIfAbsent(partition, k -> new SharedOutputBufferPartition(partition, memoryManager));
    }

    @Override
    public synchronized ListenableFuture<?> enqueue(Page page)
    {
        return enqueue(BROADCAST_PARTITION_ID, page);
    }

    @Override
    public synchronized ListenableFuture<?> enqueue(int partition, Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after no more pages is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        SharedOutputBufferPartition partitionBuffer = createOrGetPartitionBuffer(partition);
        partitionBuffer.enqueuePage(page);
        processPendingReads();
        updateState();
        return memoryManager.getNotFullFuture();
    }

    @Override
    public synchronized CompletableFuture<BufferResult> get(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "outputId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        // if no buffers can be added, and the requested buffer does not exist, return a closed empty result
        // this can happen with limit queries
        BufferState state = this.state.get();
        if (state != FAILED && !state.canAddBuffers() && namedBuffers.get(bufferId) == null) {
            return completedFuture(emptyResults(taskInstanceId, 0, true));
        }

        // return a future for data
        GetBufferResult getBufferResult = new GetBufferResult(bufferId, startingSequenceId, maxSize);
        stateChangeListeners.add(getBufferResult);
        updateState();
        return getBufferResult.getFuture();
    }

    @Override
    public synchronized void abort(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "outputId is null");

        abortedBuffers.add(bufferId);

        NamedBuffer namedBuffer = namedBuffers.get(bufferId);
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

        partitionBuffers.values().forEach(SharedOutputBufferPartition::destroy);
        // free readers
        namedBuffers.values().forEach(SharedOutputBuffer.NamedBuffer::abort);
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
        partitionBuffers.values().forEach(SharedOutputBufferPartition::destroy);

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

            // advanced master queue
            if (!state.canAddBuffers() && !namedBuffers.isEmpty()) {
                for (Map.Entry<Integer, Set<NamedBuffer>> entry : partitionToNamedBuffer.entrySet()) {
                    SharedOutputBufferPartition partition = partitionBuffers.get(entry.getKey());
                    long newMasterSequenceId = entry.getValue().stream()
                            .mapToLong(NamedBuffer::getSequenceId)
                            .min()
                            .getAsLong();
                    partition.advanceSequenceId(newMasterSequenceId);
                }
            }

            if (!state.canAddPages()) {
                memoryManager.setNoBlockOnFull();
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

    @ThreadSafe
    private final class NamedBuffer
    {
        private final OutputBufferId bufferId;
        private final SharedOutputBufferPartition partition;

        private final AtomicLong sequenceId = new AtomicLong();
        private final AtomicBoolean finished = new AtomicBoolean();

        private NamedBuffer(OutputBufferId bufferId, SharedOutputBufferPartition partition)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.partition = requireNonNull(partition, "partitionBuffer is null");
        }

        public BufferInfo getInfo()
        {
            //
            // NOTE: this code must be lock free to we are not hanging state machine updates
            //
            checkDoesNotHoldLock();

            long sequenceId = this.sequenceId.get();

            if (finished.get()) {
                return new BufferInfo(bufferId, true, 0, sequenceId, partition.getInfo());
            }

            int bufferedPages = Math.max(Ints.checkedCast(partition.getPageCount() - sequenceId), 0);
            return new BufferInfo(bufferId, finished.get(), bufferedPages, sequenceId, partition.getInfo());
        }

        public long getSequenceId()
        {
            checkHoldsLock();

            return sequenceId.get();
        }

        public BufferResult getPages(long startingSequenceId, DataSize maxSize)
        {
            checkHoldsLock();
            checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

            long sequenceId = this.sequenceId.get();
            checkArgument(startingSequenceId >= sequenceId, "startingSequenceId is before the beginning of the buffer");

            // acknowledge previous pages
            if (startingSequenceId > sequenceId) {
                this.sequenceId.set(startingSequenceId);
                sequenceId = startingSequenceId;
            }

            if (isFinished()) {
                return emptyResults(taskInstanceId, startingSequenceId, true);
            }

            List<Page> pages = partition.getPages(maxSize, sequenceId);

            // if we can't have any more pages, indicate that the buffer is complete
            if (pages.isEmpty() && !state.get().canAddPages() && !partition.hasMorePages(sequenceId)) {
                return emptyResults(taskInstanceId, startingSequenceId, true);
            }

            return new BufferResult(taskInstanceId, startingSequenceId, startingSequenceId + pages.size(), false, pages);
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
                    .add("sequenceId", sequenceId.get())
                    .add("finished", finished.get())
                    .toString();
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
                    future.complete(emptyResults(taskInstanceId, namedBuffer == null ? 0 : namedBuffer.getSequenceId(), true));
                    return true;
                }

                // buffer doesn't exist yet. Block reader until buffer is created
                if (namedBuffer == null) {
                    return false;
                }

                // if request is for pages before the current position, just return an empty page
                if (startingSequenceId < namedBuffer.getSequenceId()) {
                    future.complete(emptyResults(taskInstanceId, startingSequenceId, false));
                    return true;
                }

                // read pages from the buffer
                BufferResult bufferResult = namedBuffer.getPages(startingSequenceId, maxSize);

                // if this was the last page, we're done
                checkFlushComplete();

                // if we got an empty result, wait for more pages
                if (bufferResult.isEmpty() && !bufferResult.isBufferComplete()) {
                    return false;
                }

                future.complete(bufferResult);
            }
            catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
            return true;
        }
    }
}
