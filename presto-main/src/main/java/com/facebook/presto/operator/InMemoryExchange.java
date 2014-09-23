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
package com.facebook.presto.operator;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@ThreadSafe
public class InMemoryExchange
{
    private final List<Type> types;
    private final Queue<Page> buffer;
    private final long maxBufferedBytes;

    @GuardedBy("this")
    private boolean finishing;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private int sinkFactories;

    @GuardedBy("this")
    private int sinks;

    @GuardedBy("this")
    private long bufferBytes;

    @GuardedBy("this")
    private SettableFuture<?> readerFuture;

    @GuardedBy("this")
    private SettableFuture<?> writerFuture;

    public InMemoryExchange(List<Type> types)
    {
        this(types, new DataSize(32, MEGABYTE));
    }

    public InMemoryExchange(List<Type> types, DataSize maxBufferedBytes)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.buffer = new ConcurrentLinkedQueue<>();

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferedBytes must be greater than zero");
        this.maxBufferedBytes = maxBufferedBytes.toBytes();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public synchronized OperatorFactory createSinkFactory(int operatorId)
    {
        sinkFactories++;
        return new InMemoryExchangeSinkOperatorFactory(operatorId);
    }

    private synchronized void addSink()
    {
        checkState(sinkFactories > 0, "All sink factories already closed");
        sinks++;
    }

    public synchronized void sinkFinished()
    {
        checkState(sinks != 0, "All sinks are already complete");
        sinks--;
        updateState();
    }

    public synchronized void noMoreSinkFactories()
    {
        this.noMoreSinkFactories = true;
        updateState();
    }

    private synchronized void sinkFactoryClosed()
    {
        checkState(sinkFactories != 0, "All sinks factories are already closed");
        sinkFactories--;
        updateState();
    }

    private void updateState()
    {
        if (noMoreSinkFactories && (sinkFactories == 0) && (sinks == 0)) {
            finish();
        }
    }

    public synchronized boolean isFinishing()
    {
        return finishing;
    }

    public synchronized void finish()
    {
        finishing = true;
        notifyBlockedReaders();
        notifyBlockedWriters();
    }

    public synchronized boolean isFinished()
    {
        return finishing && buffer.isEmpty();
    }

    public synchronized void addPage(Page page)
    {
        if (finishing) {
            return;
        }
        page.assureLoaded();
        buffer.add(page);
        bufferBytes += page.getDataSize().toBytes();
        // TODO: record memory usage using OperatorContext.setMemoryReservation()
        notifyBlockedReaders();
    }

    private synchronized void notifyBlockedReaders()
    {
        if (readerFuture != null) {
            readerFuture.set(null);
            readerFuture = null;
        }
    }

    public synchronized ListenableFuture<?> waitForReading()
    {
        if (finishing || !buffer.isEmpty()) {
            return NOT_BLOCKED;
        }
        if (readerFuture == null) {
            readerFuture = SettableFuture.create();
        }
        return readerFuture;
    }

    public synchronized Page removePage()
    {
        Page page = buffer.poll();
        if (page != null) {
            bufferBytes -= page.getDataSize().toBytes();
        }
        if (bufferBytes < maxBufferedBytes) {
            notifyBlockedWriters();
        }
        return page;
    }

    private synchronized void notifyBlockedWriters()
    {
        if (writerFuture != null) {
            writerFuture.set(null);
            writerFuture = null;
        }
    }

    public synchronized ListenableFuture<?> waitForWriting()
    {
        if (bufferBytes < maxBufferedBytes) {
            return NOT_BLOCKED;
        }
        if (writerFuture == null) {
            writerFuture = SettableFuture.create();
        }
        return writerFuture;
    }

    private class InMemoryExchangeSinkOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private boolean closed;

        private InMemoryExchangeSinkOperatorFactory(int operatorId)
        {
            this.operatorId = operatorId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, InMemoryExchangeSinkOperator.class.getSimpleName());
            addSink();
            return new InMemoryExchangeSinkOperator(operatorContext, InMemoryExchange.this);
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                sinkFactoryClosed();
            }
        }
    }
}
