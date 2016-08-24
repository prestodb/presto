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
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class MemoryPagesStore
{
    private final Map<Long, List<Page>> pages = new HashMap<>();

    public synchronized void add(Long tableId, Page page)
    {
        if (!pages.containsKey(tableId)) {
            pages.put(tableId, new ArrayList<>());
        }

        List<Page> tablePages = pages.get(tableId);
        tablePages.add(page);
    }

    public synchronized List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        checkState(pages.containsKey(tableId));

        List<Page> tablePages = pages.get(tableId);
        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        for (int i = partNumber; i < tablePages.size(); i += totalParts) {
            partitionedPages.add(reorderPage(tablePages.get(i), columnIndexes));
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
        // that have smaller value then max(activeTableIds).

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId  = Collections.max(activeTableIds);

        for (Long tableId : ImmutableList.copyOf(pages.keySet())) {
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                pages.remove(tableId);
            }
        }
    }

    private static Page reorderPage(Page page, List<Integer> columnIndexes)
    {
        Block[] blocks = page.getBlocks();
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = blocks[columnIndexes.get(i)];
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }
}
