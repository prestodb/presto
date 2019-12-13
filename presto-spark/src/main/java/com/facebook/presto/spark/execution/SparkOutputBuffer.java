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
package com.facebook.presto.spark.execution;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class SparkOutputBuffer
        implements OutputBuffer
{
    private static final ListenableFuture<?> NON_BLOCKED = immediateFuture(null);

    private final StateMachine<BufferState> state = new StateMachine<>("spark buffer", directExecutor(), NO_MORE_BUFFERS, TERMINAL_BUFFER_STATES);

    private final Queue<PageWithPartition> bufferedPages = new ConcurrentLinkedQueue<>();

    @Override
    public OutputBufferInfo getInfo()
    {
        BufferState state = this.state.get();

        return new OutputBufferInfo(
                "SPARK",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                0,
                0,
                0,
                0,
                ImmutableList.of());
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return NON_BLOCKED;
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        enqueue(lifespan, 0, pages);
    }

    @Override
    public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        checkState(state.get() == NO_MORE_BUFFERS, "unexpected state: %s", state.get());
        pages.forEach(page -> bufferedPages.add(new PageWithPartition(page, partition)));
    }

    @Override
    public void setNoMorePages()
    {
        state.setIf(FLUSHING, state -> state == NO_MORE_BUFFERS);
    }

    public boolean hasPagesBuffered()
    {
        return !bufferedPages.isEmpty();
    }

    public PageWithPartition getNext()
    {
        PageWithPartition page = bufferedPages.poll();
        if (bufferedPages.isEmpty()) {
            state.setIf(FINISHED, state -> state == FLUSHING);
        }
        return page;
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fail()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return 0;
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
}
