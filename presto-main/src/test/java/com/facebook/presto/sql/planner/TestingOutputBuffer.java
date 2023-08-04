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
package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.function.Consumer;

public class TestingOutputBuffer
        implements OutputBuffer
{
    @Override
    public OutputBufferInfo getInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getUtilization()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOverutilized()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(OutputBuffers.OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNoMorePages()
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
        throw new UnsupportedOperationException();
    }
}
