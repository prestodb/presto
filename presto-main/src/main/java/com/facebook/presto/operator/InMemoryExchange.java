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

import com.facebook.presto.spi.Page;
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
    private static final DataSize DEFAULT_MAX_BUFFERED_BYTES = new DataSize(32, MEGABYTE);
    private final List<Type> types;
    private final List<Queue<PageReference>> buffers;
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
        this(types, 1, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public InMemoryExchange(List<Type> types, int bufferCount)
    {
        this(types, bufferCount, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public InMemoryExchange(List<Type> types, int bufferCount, DataSize maxBufferedBytes)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));

        ImmutableList.Builder<Queue<PageReference>> buffers = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            buffers.add(new ConcurrentLinkedQueue<>());
        }
        this.buffers = buffers.build();

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferedBytes must be greater than zero");
        this.maxBufferedBytes = maxBufferedBytes.toBytes();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getBufferCount()
    {
        return buffers.size();
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

    public synchronized boolean isFinished(int bufferIndex)
    {
        return finishing && buffers.get(bufferIndex).isEmpty();
    }

    public synchronized void addPage(Page page)
    {
        if (finishing) {
            return;
        }
        PageReference pageReference = new PageReference(page, buffers.size());
        for (Queue<PageReference> buffer : buffers) {
            buffer.add(pageReference);
        }
        bufferBytes += page.getSizeInBytes();
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

    public synchronized ListenableFuture<?> waitForReading(int bufferIndex)
    {
        if (finishing || !buffers.get(bufferIndex).isEmpty()) {
            return NOT_BLOCKED;
        }
        if (readerFuture == null) {
            readerFuture = SettableFuture.create();
        }
        return readerFuture;
    }

    public synchronized Page removePage(int bufferIndex)
    {
        PageReference pageReference = buffers.get(bufferIndex).poll();
        if (pageReference == null) {
            return null;
        }

        Page page = pageReference.removePage();
        if (!pageReference.isReferenced()) {
            bufferBytes -= page.getSizeInBytes();
            if (bufferBytes < maxBufferedBytes) {
                notifyBlockedWriters();
            }
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

    private static class PageReference
    {
        private final Page page;
        private int referenceCount;

        public PageReference(Page page, int referenceCount)
        {
            this.page = page;
            this.referenceCount = referenceCount;
        }

        public Page removePage()
        {
            checkArgument(referenceCount > 0);
            referenceCount--;
            return page;
        }

        public boolean isReferenced()
        {
            return referenceCount > 0;
        }
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
