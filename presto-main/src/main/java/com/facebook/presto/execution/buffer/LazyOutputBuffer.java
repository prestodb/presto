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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ExtendedSettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

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
    private final String taskInstanceId;
    private final DataSize maxBufferSize;
    private final SystemMemoryUsageListener systemMemoryUsageListener;
    private final Executor executor;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    public LazyOutputBuffer(
            TaskId taskId,
            String taskInstanceId,
            Executor executor,
            DataSize maxBufferSize,
            SystemMemoryUsageListener systemMemoryUsageListener)
    {
        requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.executor = requireNonNull(executor, "executor is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.systemMemoryUsageListener = requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null");
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
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        // until output buffer is initialized, it is "full"
        if (outputBuffer == null) {
            return 1.0;
        }
        return outputBuffer.getUtilization();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

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
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore set output if buffer was already destroyed or failed
                if (state.get().isTerminal()) {
                    return;
                }
                switch (newOutputBuffers.getType()) {
                    case PARTITIONED:
                        delegate = new PartitionedOutputBuffer(taskInstanceId, state, newOutputBuffers, maxBufferSize, systemMemoryUsageListener, executor);
                        break;
                    case BROADCAST:
                        delegate = new BroadcastOutputBuffer(taskInstanceId, state, maxBufferSize, systemMemoryUsageListener, executor);
                        break;
                    case ARBITRARY:
                        delegate = new ArbitraryOutputBuffer(taskInstanceId, state, maxBufferSize, systemMemoryUsageListener, executor);
                        break;
                }

                // process pending aborts and reads outside of synchronized lock
                abortedBuffers = ImmutableSet.copyOf(this.abortedBuffers);
                this.abortedBuffers.clear();
                pendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
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
        OutputBuffer outputBuffer;
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
        return outputBuffer.get(bufferId, token, maxSize);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(bufferId);
                // Normally, we should free any pending readers for this buffer,
                // but we assume that the real buffer will be created quickly.
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(bufferId);
    }

    @Override
    public ListenableFuture<?> enqueue(List<SerializedPage> pages)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(pages);
    }

    @Override
    public ListenableFuture<?> enqueue(int partition, List<SerializedPage> pages)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(partition, pages);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMorePages();
    }

    @Override
    public void destroy()
    {
        OutputBuffer outputBuffer;
        List<PendingRead> pendingReads = ImmutableList.of();
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
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                state.setIf(FAILED, state -> !state.isTerminal());

                // Do not free readers on fail
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.fail();
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
