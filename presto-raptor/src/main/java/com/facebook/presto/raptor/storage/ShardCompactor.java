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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.Row.extractRow;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

public final class ShardCompactor
{
    private final StorageManager storageManager;

    @Inject
    public ShardCompactor(StorageManager storageManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    public List<ShardInfo> compact(Set<UUID> uuids, List<ColumnInfo> columns)
            throws IOException
    {
        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        StoragePageSink storagePageSink = storageManager.createStoragePageSink(columnIds, columnTypes);
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, columnIds, columnTypes, TupleDomain.all())) {
                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (isNullOrEmptyPage(page)) {
                        continue;
                    }
                    storagePageSink.appendPages(ImmutableList.of(page));
                    if (storagePageSink.isFull()) {
                        storagePageSink.flush();
                    }
                }
            }
        }
        return storagePageSink.commit();
    }

    public List<ShardInfo> compactSorted(Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, List<SortOrder> sortOrders)
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");
        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        checkArgument(columnIds.containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");
        List<Integer> sortIndexes = ImmutableList.copyOf(sortColumnIds.stream().map(sortColumnIds::indexOf).collect(toList()));

        Queue<SortedRowSource> rowSources = new PriorityQueue<>();
        StoragePageSink outputPageSink = storageManager.createStoragePageSink(columnIds, columnTypes);
        try {
            for (UUID uuid : uuids) {
                ConnectorPageSource pageSource = storageManager.getPageSource(uuid, columnIds, columnTypes, TupleDomain.all());
                SortedRowSource rowSource = new SortedRowSource(pageSource, columnTypes, sortIndexes, sortOrders);
                rowSources.add(rowSource);
            }
            while (!rowSources.isEmpty()) {
                SortedRowSource rowSource = rowSources.poll();
                if (!rowSource.hasNext()) {
                    // rowSource is empty, close it
                    rowSource.close();
                    continue;
                }

                outputPageSink.appendRow(rowSource.next());

                if (outputPageSink.isFull()) {
                    outputPageSink.flush();
                }

                rowSources.add(rowSource);
            }
            outputPageSink.flush();
            return outputPageSink.commit();
        }
        catch (IOException exception) {
            throw Throwables.propagate(exception);
        }
        finally {
            outputPageSink.flush();
            rowSources.stream().forEach(SortedRowSource::closeQuietly);
        }
    }

    private static class SortedRowSource
            implements Iterator<Row>, Comparable<SortedRowSource>, Closeable
    {
        private final ConnectorPageSource pageSource;
        private final List<Type> columnTypes;
        private final List<Integer> sortIndexes;
        private final List<SortOrder> sortOrders;

        private Page currentPage;
        private int currentPosition;

        public SortedRowSource(ConnectorPageSource pageSource, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
        {
            this.pageSource = checkNotNull(pageSource, "pageSource is null");
            this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
            this.sortIndexes = ImmutableList.copyOf(checkNotNull(sortIndexes, "sortIndexes is null"));
            this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));

            currentPage = pageSource.getNextPage();
            currentPosition = 0;
        }

        @Override
        public boolean hasNext()
        {
            if (hasMorePositions(currentPage, currentPosition)) {
                return true;
            }

            Page page = getNextPage(pageSource);
            if (isNullOrEmptyPage(page)) {
                return false;
            }
            page.assureLoaded();
            currentPage = page;
            currentPosition = 0;
            return true;
        }

        private static Page getNextPage(ConnectorPageSource pageSource)
        {
            Page page = null;
            while (isNullOrEmptyPage(page) && !pageSource.isFinished()) {
                page = pageSource.getNextPage();
            }
            return page;
        }

        @Override
        public Row next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Row row = extractRow(currentPage, currentPosition, columnTypes);
            currentPosition++;
            return row;
        }

        @Override
        public int compareTo(SortedRowSource other)
        {
            if (!hasNext()) {
                return 1;
            }

            for (int i = 0; i < sortIndexes.size(); i++) {
                int index = sortIndexes.get(i);

                Block leftBlock = currentPage.getBlock(index);
                int leftBlockPosition = currentPosition;

                Block rightBlock = other.currentPage.getBlock(index);
                int rightBlockPosition = other.currentPosition;

                int compare = sortOrders.get(i).compareBlockValue(columnTypes.get(i), leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }

        private static boolean hasMorePositions(Page currentPage, int currentPosition)
        {
            return currentPage != null && currentPosition < currentPage.getPositionCount();
        }

        void closeQuietly()
        {
            try {
                close();
            }
            catch (IOException ignored) {
            }
        }

        @Override
        public void close()
                throws IOException
        {
            pageSource.close();
        }
    }

    private static boolean isNullOrEmptyPage(Page nextPage)
    {
        return nextPage == null || nextPage.getPositionCount() == 0;
    }
}
