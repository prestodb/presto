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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.Row.extractRow;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class ShardCompactor
{
    private final StorageManager storageManager;

    private final CounterStat inputShards = new CounterStat();
    private final CounterStat outputShards = new CounterStat();
    private final DistributionStat inputShardsPerCompaction = new DistributionStat();
    private final DistributionStat outputShardsPerCompaction = new DistributionStat();
    private final DistributionStat compactionLatencyMillis = new DistributionStat();
    private final DistributionStat sortedCompactionLatencyMillis = new DistributionStat();
    private final ReaderAttributes readerAttributes;

    @Inject
    public ShardCompactor(StorageManager storageManager, ReaderAttributes readerAttributes)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.readerAttributes = requireNonNull(readerAttributes, "readerAttributes is null");
    }

    public List<ShardInfo> compact(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns)
            throws IOException
    {
        long start = System.nanoTime();
        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        StoragePageSink storagePageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes);

        List<ShardInfo> shardInfos;
        try {
            shardInfos = compact(storagePageSink, bucketNumber, uuids, columnIds, columnTypes);
        }
        catch (IOException | RuntimeException e) {
            storagePageSink.rollback();
            throw e;
        }

        updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());
        return shardInfos;
    }

    private List<ShardInfo> compact(StoragePageSink storagePageSink, OptionalInt bucketNumber, Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes)) {
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

    public List<ShardInfo> compactSorted(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, List<SortOrder> sortOrders)
            throws IOException
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");

        long start = System.nanoTime();

        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        checkArgument(columnIds.containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");

        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        Queue<SortedRowSource> rowSources = new PriorityQueue<>();
        StoragePageSink outputPageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes);
        try {
            for (UUID uuid : uuids) {
                ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes);
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
            List<ShardInfo> shardInfos = outputPageSink.commit();

            updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());

            return shardInfos;
        }
        catch (IOException | RuntimeException e) {
            outputPageSink.rollback();
            throw e;
        }
        finally {
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
            this.pageSource = requireNonNull(pageSource, "pageSource is null");
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.sortIndexes = ImmutableList.copyOf(requireNonNull(sortIndexes, "sortIndexes is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

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
                if (page != null) {
                    page.assureLoaded();
                }
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

            if (!other.hasNext()) {
                return -1;
            }

            for (int i = 0; i < sortIndexes.size(); i++) {
                int channel = sortIndexes.get(i);
                Type type = columnTypes.get(channel);

                Block leftBlock = currentPage.getBlock(channel);
                int leftBlockPosition = currentPosition;

                Block rightBlock = other.currentPage.getBlock(channel);
                int rightBlockPosition = other.currentPosition;

                int compare = sortOrders.get(i).compareBlockValue(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
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

    private void updateStats(int inputShardsCount, int outputShardsCount, long latency)
    {
        inputShards.update(inputShardsCount);
        outputShards.update(outputShardsCount);

        inputShardsPerCompaction.add(inputShardsCount);
        outputShardsPerCompaction.add(outputShardsCount);

        compactionLatencyMillis.add(latency);
    }

    @Managed
    @Nested
    public CounterStat getInputShards()
    {
        return inputShards;
    }

    @Managed
    @Nested
    public CounterStat getOutputShards()
    {
        return outputShards;
    }

    @Managed
    @Nested
    public DistributionStat getInputShardsPerCompaction()
    {
        return inputShardsPerCompaction;
    }

    @Managed
    @Nested
    public DistributionStat getOutputShardsPerCompaction()
    {
        return outputShardsPerCompaction;
    }

    @Managed
    @Nested
    public DistributionStat getCompactionLatencyMillis()
    {
        return compactionLatencyMillis;
    }

    @Managed
    @Nested
    public DistributionStat getSortedCompactionLatencyMillis()
    {
        return sortedCompactionLatencyMillis;
    }
}
