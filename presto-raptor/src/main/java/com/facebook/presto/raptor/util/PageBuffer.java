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
package com.facebook.presto.raptor.util;

import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageBuffer
{
    private final long maxMemoryBytes;
    private final StoragePageSink storagePageSink;
    private final List<Type> columnTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final PageSorter pageSorter;
    private final List<Page> pages = new ArrayList<>();

    private long usedMemoryBytes;
    private long rowCount;

    public PageBuffer(
            long maxMemoryBytes,
            StoragePageSink storagePageSink,
            List<Type> columnTypes,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter)
    {
        checkArgument(maxMemoryBytes > 0, "maxMemoryBytes must be positive");
        this.maxMemoryBytes = maxMemoryBytes;
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.storagePageSink = requireNonNull(storagePageSink, "storagePageSink is null");
    }

    public StoragePageSink getStoragePageSink()
    {
        return storagePageSink;
    }

    public long getUsedMemoryBytes()
    {
        return usedMemoryBytes;
    }

    public void add(Page page)
    {
        flushIfNecessary(page.getPositionCount());
        pages.add(page);
        usedMemoryBytes += page.getSizeInBytes();
        rowCount += page.getPositionCount();
    }

    public void flush()
    {
        if (pages.isEmpty()) {
            return;
        }

        if (sortFields.isEmpty()) {
            storagePageSink.appendPages(pages);
        }
        else {
            appendSorted();
        }

        storagePageSink.flush();

        pages.clear();
        rowCount = 0;
        usedMemoryBytes = 0;
    }

    private void appendSorted()
    {
        long[] addresses = pageSorter.sort(columnTypes, pages, sortFields, sortOrders, toIntExact(rowCount));

        int[] pageIndex = new int[addresses.length];
        int[] positionIndex = new int[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            pageIndex[i] = pageSorter.decodePageIndex(addresses[i]);
            positionIndex[i] = pageSorter.decodePositionIndex(addresses[i]);
        }

        storagePageSink.appendPages(pages, pageIndex, positionIndex);
    }

    private void flushIfNecessary(int rowsToAdd)
    {
        if (storagePageSink.isFull() || !canAddRows(rowsToAdd)) {
            flush();
        }
    }

    private boolean canAddRows(int rowsToAdd)
    {
        return (usedMemoryBytes < maxMemoryBytes) &&
                ((rowCount + rowsToAdd) < Integer.MAX_VALUE);
    }
}
