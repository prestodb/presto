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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static com.facebook.presto.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes = 0;

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    private final Map<Long, List<Page>> pages = new HashMap<>();

    public synchronized void initialize(long tableId)
    {
        if (!pages.containsKey(tableId)) {
            pages.put(tableId, new ArrayList<>());
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        List<Page> tablePages = pages.get(tableId);
        tablePages.add(page);
    }

    public synchronized List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        List<Page> tablePages = pages.get(tableId);
        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        for (int i = partNumber; i < tablePages.size(); i += totalParts) {
            partitionedPages.add(getColumns(tablePages.get(i), columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return pages.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId  = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, List<Page>>> tablePages = pages.entrySet().iterator(); tablePages.hasNext(); ) {
            Map.Entry<Long, List<Page>> tablePagesEntry = tablePages.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tablePages.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] blocks = page.getBlocks();
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = blocks[columnIndexes.get(i)];
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }
}
