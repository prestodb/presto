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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

@ThreadSafe
public class LocalMergeExchange
{
    private static final int DEFAULT_MAX_BUFFERED_PAGES = 2;

    private final List<ExchangeBuffer> buffers;

    public LocalMergeExchange(int buffersCount)
    {
        this(buffersCount, DEFAULT_MAX_BUFFERED_PAGES);
    }

    public LocalMergeExchange(int buffersCount, int maxBufferedPagesPerBuffer)
    {
        checkArgument(buffersCount > 0, "buffersCount must be greater than zero");
        checkArgument(maxBufferedPagesPerBuffer > 0, "maxBufferedPagesPerBuffer must be greater than zero");
        buffers = IntStream.range(0, buffersCount)
                .mapToObj((i) -> new ExchangeBuffer(maxBufferedPagesPerBuffer))
                .collect(toImmutableList());
    }

    public List<ExchangeBuffer> getBuffers()
    {
        return buffers;
    }

    public ExchangeBuffer getBuffer(int index)
    {
        checkElementIndex(index, buffers.size());
        return buffers.get(index);
    }

    public void finishRead()
    {
        buffers.forEach(ExchangeBuffer::finishRead);
    }

    public void close()
    {
        finishRead();
    }

    @ThreadSafe
    public static class ExchangeBuffer
    {
        private final Object lock = new Object();
        private final int maxBufferedPages;

        @GuardedBy("lock")
        private final Queue<Page> pages = new LinkedList<>();
        @GuardedBy("lock")
        private SettableFuture<?> readBlocked = SettableFuture.create();
        @GuardedBy("lock")
        private SettableFuture<?> writeBlocked = SettableFuture.create();
        @GuardedBy("lock")
        private boolean readFinished;
        @GuardedBy("lock")
        private boolean writeFinished;

        public ExchangeBuffer(int maxBufferedPages)
        {
            checkArgument(maxBufferedPages > 0, "maxBufferedPages must be greater then zero");
            this.maxBufferedPages = maxBufferedPages;
            writeBlocked.set(null);
        }

        public ListenableFuture<?> isWriteBlocked()
        {
            synchronized (lock) {
                return writeBlocked;
            }
        }

        public void enqueuePage(Page page)
        {
            synchronized (lock) {
                checkState(!writeFinished, "write is finished");
                checkState(writeBlocked.isDone(), "write is blocked");
                if (readFinished) {
                    return;
                }
                pages.add(page);
                readBlocked.set(null);
                if (isFull()) {
                    writeBlocked = SettableFuture.create();
                }
            }
        }

        public ListenableFuture<?> isReadBlocked()
        {
            synchronized (lock) {
                return readBlocked;
            }
        }

        public Page poolPage()
        {
            synchronized (lock) {
                checkState(!readFinished, "finished");
                checkState(readBlocked.isDone(), "read is blocked");
                Page page = pages.poll();
                if (!writeFinished && pages.isEmpty()) {
                    readBlocked = SettableFuture.create();
                }
                if (!isFull()) {
                    writeBlocked.set(null);
                }
                return page;
            }
        }

        public void finishRead()
        {
            synchronized (lock) {
                readFinished = true;
                readBlocked.set(null);
                writeBlocked.set(null);
            }
        }

        public void finishWrite()
        {
            synchronized (lock) {
                writeFinished = true;
                readBlocked.set(null);
                writeBlocked.set(null);
            }
        }

        public boolean isReadFinished()
        {
            synchronized (lock) {
                return readFinished;
            }
        }

        public boolean isWriteFinished()
        {
            synchronized (lock) {
                return writeFinished;
            }
        }

        private boolean isFull()
        {
            checkState(Thread.holdsLock(lock));
            return pages.size() >= maxBufferedPages;
        }

        public boolean isEmpty()
        {
            synchronized (lock) {
                return pages.isEmpty();
            }
        }
    }
}
