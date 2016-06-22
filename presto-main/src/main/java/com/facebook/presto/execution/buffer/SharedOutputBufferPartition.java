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

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SharedOutputBufferPartition
{
    private final LinkedList<Page> masterBuffer = new LinkedList<>();
    private final AtomicLong rowsAdded = new AtomicLong(); // Number of rows added to the masterBuffer
    private final AtomicLong pagesAdded = new AtomicLong(); // Number of pages added to the masterBuffer
    private final AtomicLong masterSequenceId = new AtomicLong();
    private final AtomicLong bufferedBytes = new AtomicLong();  // Bytes in the master buffer
    private final int partition;
    private final OutputBufferMemoryManager memoryManager;

    public SharedOutputBufferPartition(int partition, OutputBufferMemoryManager memoryManager)
    {
        checkArgument(partition >= 0, "partition must be >= 0");
        this.partition = partition;
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    public synchronized void enqueuePage(Page page)
    {
        List<Page> pages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        masterBuffer.addAll(pages);

        long rowCount = pages.stream().mapToLong(Page::getPositionCount).sum();
        checkState(rowCount == page.getPositionCount());
        rowsAdded.addAndGet(rowCount);
        pagesAdded.addAndGet(pages.size());

        long bytesAdded = 0;
        for (Page p : pages) {
            bytesAdded += p.getRetainedSizeInBytes();
        }
        updateMemoryUsage(bytesAdded);
    }

    /**
     * @return at least one page if we have pages in buffer, empty list otherwise
     */
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

    /**
     * @return true if there are still pages in the buffer after sequenceId
     */
    public synchronized boolean hasMorePages(long sequenceId)
    {
        int listOffset = Ints.checkedCast(sequenceId - masterSequenceId.get());
        return listOffset < masterBuffer.size();
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
            bytesRemoved += page.getRetainedSizeInBytes();
        }
        updateMemoryUsage(-bytesRemoved);
    }

    public synchronized void destroy()
    {
        // clear the buffer
        masterBuffer.clear();
        updateMemoryUsage(-bufferedBytes.get());
    }

    private void updateMemoryUsage(long bytesAdded)
    {
        bufferedBytes.addAndGet(bytesAdded);
        memoryManager.updateMemoryUsage(bytesAdded);
        verify(bufferedBytes.get() >= 0);
    }

    public long getRowCount()
    {
        return rowsAdded.get();
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

    public int getPartition()
    {
        return partition;
    }

    public PageBufferInfo getInfo()
    {
        return new PageBufferInfo(partition, getBufferedPageCount(), getBufferedBytes(), rowsAdded.get(), pagesAdded.get());
    }
}
