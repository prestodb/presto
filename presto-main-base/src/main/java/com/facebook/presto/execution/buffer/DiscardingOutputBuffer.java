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
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class DiscardingOutputBuffer
        implements OutputBuffer
{
    private static final ListenableFuture<?> NON_BLOCKED = immediateFuture(null);

    private final OutputBuffers outputBuffers;
    private final StateMachine<BufferState> state;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public DiscardingOutputBuffer(OutputBuffers outputBuffers, StateMachine<BufferState> state)
    {
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.state = requireNonNull(state, "state is null");
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        BufferState state = this.state.get();

        return new OutputBufferInfo(
                "DISCARD",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                0,
                0,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                ImmutableList.of());
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return 0;
    }

    @Override
    public boolean isOverutilized()
    {
        return false;
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

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException("DiscardingOutputBuffer must not have any active readers");
    }

    @Override
    public void acknowledge(OutputBuffers.OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException("DiscardingOutputBuffer must not have any active readers");
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId) {}

    @Override
    public ListenableFuture<?> isFull()
    {
        return NON_BLOCKED;
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        // update stats
        long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());
    }

    @Override
    public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        checkState(partition == 0, "Expected partition number to be zero");
        enqueue(lifespan, pages);
    }

    @Override
    public void setNoMorePages()
    {
        state.set(FINISHED);
    }

    @Override
    public void destroy()
    {
        state.setIf(FINISHED, oldState -> !oldState.isTerminal());
    }

    @Override
    public void fail()
    {
        state.setIf(FAILED, oldState -> !oldState.isTerminal());
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
        return true;
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return 0;
    }
}
