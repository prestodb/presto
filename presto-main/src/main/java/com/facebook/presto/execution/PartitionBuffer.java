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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.execution.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionBuffer
{
    private final LinkedList<Page> masterBuffer = new LinkedList<>();
    private final BlockingQueue<QueuedPage> queuedPages = new LinkedBlockingQueue<>();
    private final AtomicLong pagesAdded = new AtomicLong(); // Number of pages added to the masterBuffer
    private final AtomicLong masterSequenceId = new AtomicLong();
    private final AtomicLong bufferedBytes = new AtomicLong();  // Bytes in the master buffer
    private final int partition;
    private final SharedBufferMemoryManager memoryManager;

    public PartitionBuffer(int partition, SharedBufferMemoryManager memoryManager)
    {
        checkArgument(partition >= 0, "partition must be >= 0");
        this.partition = partition;
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    public synchronized ListenableFuture<?> enqueuePage(Page page)
    {
        if (!memoryManager.isFull()) {
            addToMasterBuffer(page);
            return immediateFuture(true);
        }
        else {
            QueuedPage queuedPage = new QueuedPage(page);
            queuedPages.add(queuedPage);
            return queuedPage.getFuture();
        }
    }

    private synchronized void addToMasterBuffer(Page page)
    {
        long bytesAdded = 0;
        List<Page> pages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        masterBuffer.addAll(pages);
        pagesAdded.addAndGet(pages.size());
        for (Page p : pages) {
            bytesAdded += p.getSizeInBytes();
        }
        updateMemoryUsage(bytesAdded);
    }

    public synchronized List<Page> getPages(DataSize maxSize, long sequenceId)
    {
        long maxBytes = maxSize.toBytes();
        List<Page> pages = new ArrayList<>();
        long bytes = 0;

        int listOffset = Ints.checkedCast(sequenceId - masterSequenceId.get());
        while (listOffset < masterBuffer.size()) {
            Page page = masterBuffer.get(listOffset++);
            bytes += page.getSizeInBytes();
            // break (and don't add) if this page would exceed the limit
            if (!pages.isEmpty() && bytes > maxBytes) {
                break;
            }
            pages.add(page);
        }
        return ImmutableList.copyOf(pages);
    }

    public synchronized void advanceSequenceId(long newSequenceId)
    {
        long oldMasterSequenceId = masterSequenceId.get();
        checkArgument(newSequenceId >= oldMasterSequenceId, "Master sequence id moved backwards: oldMasterSequenceId=%s, newMasterSequenceId=%s",
                oldMasterSequenceId,
                newSequenceId);

        if (newSequenceId == oldMasterSequenceId) {
            return;
        }
        masterSequenceId.set(newSequenceId);

        // drop consumed pages
        int pagesToRemove = Ints.checkedCast(newSequenceId - oldMasterSequenceId);
        checkState(masterBuffer.size() >= pagesToRemove,
                "MasterBuffer does not have any pages to remove: pagesToRemove %s oldMasterSequenceId: %s newSequenceId: %s",
                pagesToRemove,
                oldMasterSequenceId,
                newSequenceId);
        long bytesRemoved = 0;
        for (int i = 0; i < pagesToRemove; i++) {
            Page page = masterBuffer.removeFirst();
            bytesRemoved += page.getSizeInBytes();
        }
        updateMemoryUsage(-bytesRemoved);

        // refill buffer from queued pages
        while (!queuedPages.isEmpty() && !memoryManager.isFull()) {
            QueuedPage queuedPage = queuedPages.remove();
            addToMasterBuffer(queuedPage.getPage());
            queuedPage.getFuture().set(null);
        }
    }

    public synchronized void destroy()
    {
        // clear the buffer
        masterBuffer.clear();
        updateMemoryUsage(-bufferedBytes.get());
        clearQueue();
    }

    public synchronized void clearQueue()
    {
        for (QueuedPage queuedPage : queuedPages) {
            queuedPage.getFuture().set(null);
        }
        queuedPages.clear();
    }

    private void updateMemoryUsage(long bytesAdded)
    {
        bufferedBytes.addAndGet(bytesAdded);
        memoryManager.updateMemoryUsage(bytesAdded);
        verify(bufferedBytes.get() >= 0);
    }

    public long getPageCount()
    {
        return pagesAdded.get();
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }

    public long getBufferedPageCount()
    {
        return masterBuffer.size();
    }

    public long getQueuedPageCount()
    {
        return queuedPages.size();
    }

    public int getPartition()
    {
        return partition;
    }

    public PageBufferInfo getInfo()
    {
        return new PageBufferInfo(partition, getBufferedPageCount(), getQueuedPageCount(), getBufferedBytes(), pagesAdded.get());
    }

    @Immutable
    private static final class QueuedPage
    {
        private final Page page;
        private final SettableFuture<?> future = SettableFuture.create();

        QueuedPage(Page page)
        {
            this.page = page;
        }

        public Page getPage()
        {
            return page;
        }

        public SettableFuture<?> getFuture()
        {
            return future;
        }
    }
}
