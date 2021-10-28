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

import com.facebook.airlift.concurrent.ExtendedSettableFuture;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class LazyOutputBuffer
        implements OutputBuffer
{
    private final StateMachine<BufferState> state;
    private final TaskId taskId;
    private final String taskInstanceId;
    private final DataSize maxBufferSize;
    private final Supplier<LocalMemoryContext> systemMemoryContextSupplier;
    private final Executor executor;
    private final SpoolingOutputBufferFactory spoolingOutputBufferFactory;

    // Note: this is a write once field, so an unsynchronized volatile read that returns a non-null value is safe, but if a null value is observed instead
    // a subsequent synchronized read is required to ensure the writing thread can complete any in-flight initialization
    @GuardedBy("this")
    private volatile OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    public LazyOutputBuffer(
            TaskId taskId,
            String taskInstanceId,
            Executor executor,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            SpoolingOutputBufferFactory spoolingOutputBufferFactory)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.executor = requireNonNull(executor, "executor is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.systemMemoryContextSupplier = requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null");
        this.spoolingOutputBufferFactory = requireNonNull(spoolingOutputBufferFactory, "spoolingOutputBufferFactory is null");
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
        OutputBuffer outputBuffer = getDelegateOutputBuffer();

        // until output buffer is initialized, it is "full"
        if (outputBuffer == null) {
            return 1.0;
        }
        return outputBuffer.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        OutputBuffer outputBuffer = getDelegateOutputBuffer();

        // until output buffer is initialized, readers cannot enqueue and thus cannot be blocked
        return (outputBuffer != null) && outputBuffer.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        OutputBuffer outputBuffer = getDelegateOutputBuffer();

        if (outputBuffer == null) {
            //
            // NOTE: this code must be lock free to not hanging state machine updates
            //
            BufferState state = this.state.get();

            return new OutputBufferInfo(
                    "UNINITIALIZED",
                    state,
                    state.canAddBuffers(),
                    state.canAddPages(),
                    0,
                    0,
                    0,
                    0,
                    ImmutableList.of());
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        Set<OutputBufferId> abortedBuffers = ImmutableSet.of();
        List<PendingRead> pendingReads = ImmutableList.of();
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                outputBuffer = delegate;
                if (outputBuffer == null) {
                    // ignore set output if buffer was already destroyed or failed
                    if (state.get().isTerminal()) {
                        return;
                    }
                    switch (newOutputBuffers.getType()) {
                        case PARTITIONED:
                            outputBuffer = new PartitionedOutputBuffer(taskInstanceId, state, newOutputBuffers, maxBufferSize, systemMemoryContextSupplier, executor);
                            break;
                        case BROADCAST:
                            outputBuffer = new BroadcastOutputBuffer(taskInstanceId, state, maxBufferSize, systemMemoryContextSupplier, executor);
                            break;
                        case ARBITRARY:
                            outputBuffer = new ArbitraryOutputBuffer(taskInstanceId, state, maxBufferSize, systemMemoryContextSupplier, executor);
                            break;
                        case DISCARDING:
                            outputBuffer = new DiscardingOutputBuffer(newOutputBuffers, state);
                            break;
                        case SPOOLING:
                            outputBuffer = spoolingOutputBufferFactory.createSpoolingOutputBuffer(taskId, taskInstanceId, newOutputBuffers, state);
                            break;
                    }

                    // process pending aborts and reads outside of synchronized lock
                    abortedBuffers = ImmutableSet.copyOf(this.abortedBuffers);
                    this.abortedBuffers.clear();
                    pendingReads = ImmutableList.copyOf(this.pendingReads);
                    this.pendingReads.clear();
                    // Must be assigned last to avoid a race condition with unsynchronized readers
                    delegate = outputBuffer;
                }
            }
        }

        outputBuffer.setOutputBuffers(newOutputBuffers);

        // process pending aborts and reads outside of synchronized lock
        abortedBuffers.forEach(outputBuffer::abort);
        for (PendingRead pendingRead : pendingReads) {
            pendingRead.process(outputBuffer);
        }
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    if (state.get() == FINISHED) {
                        return immediateFuture(emptyResults(taskInstanceId, 0, true));
                    }

                    PendingRead pendingRead = new PendingRead(bufferId, token, maxSize);
                    pendingReads.add(pendingRead);
                    return pendingRead.getFutureResult();
                }
                outputBuffer = delegate;
            }
        }
        return outputBuffer.get(bufferId, token, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long token)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.acknowledge(bufferId, token);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    abortedBuffers.add(bufferId);
                    // Normally, we should free any pending readers for this buffer,
                    // but we assume that the real buffer will be created quickly.
                    return;
                }
                outputBuffer = delegate;
            }
        }
        outputBuffer.abort(bufferId);
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        return outputBuffer.isFull();
    }

    @Override
    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.registerLifespanCompletionCallback(callback);
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.enqueue(lifespan, pages);
    }

    @Override
    public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.enqueue(lifespan, partition, pages);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.setNoMorePages();
    }

    @Override
    public void destroy()
    {
        List<PendingRead> pendingReads = ImmutableList.of();
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    // ignore destroy if the buffer already in a terminal state.
                    if (!state.setIf(FINISHED, state -> !state.isTerminal())) {
                        return;
                    }

                    pendingReads = ImmutableList.copyOf(this.pendingReads);
                    this.pendingReads.clear();
                }
                outputBuffer = delegate;
            }
        }

        // if there is no output buffer, free the pending reads
        if (outputBuffer == null) {
            for (PendingRead pendingRead : pendingReads) {
                pendingRead.getFutureResult().set(emptyResults(taskInstanceId, 0, true));
            }
            return;
        }

        outputBuffer.destroy();
    }

    @Override
    public void fail()
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    // ignore fail if the buffer already in a terminal state.
                    state.setIf(FAILED, state -> !state.isTerminal());

                    // Do not free readers on fail
                    return;
                }
                outputBuffer = delegate;
            }
        }
        outputBuffer.fail();
    }

    @Override
    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        outputBuffer.setNoMorePagesForLifespan(lifespan);
    }

    @Override
    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        OutputBuffer outputBuffer = getDelegateOutputBufferOrFail();
        return outputBuffer.isFinishedForLifespan(lifespan);
    }

    @Override
    public long getPeakMemoryUsage()
    {
        OutputBuffer outputBuffer = getDelegateOutputBuffer();

        if (outputBuffer != null) {
            return outputBuffer.getPeakMemoryUsage();
        }
        return 0;
    }

    @Nullable
    private OutputBuffer getDelegateOutputBuffer()
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                outputBuffer = delegate;
            }
        }
        return outputBuffer;
    }

    private OutputBuffer getDelegateOutputBufferOrFail()
    {
        OutputBuffer outputBuffer = getDelegateOutputBuffer();
        checkState(outputBuffer != null, "Buffer has not been initialized");
        return outputBuffer;
    }

    private static class PendingRead
    {
        private final OutputBufferId bufferId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final ExtendedSettableFuture<BufferResult> futureResult = ExtendedSettableFuture.create();

        public PendingRead(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public ExtendedSettableFuture<BufferResult> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                ListenableFuture<BufferResult> result = delegate.get(bufferId, startingSequenceId, maxSize);
                futureResult.setAsync(result);
            }
            catch (Exception e) {
                futureResult.setException(e);
            }
        }
    }
}
