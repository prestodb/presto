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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.CompressionType;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
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

    public List<ShardInfo> compact(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, CompressionType compressionType)
            throws IOException
    {
        long start = System.nanoTime();
        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());
        Map<Long, Type> chunkColumnTypes = columns.stream().collect(Collectors.toMap(ColumnInfo::getColumnId, ColumnInfo::getType));

        StoragePageSink storagePageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, false, compressionType);

        List<ShardInfo> shardInfos;
        try {
            shardInfos = compact(storagePageSink, bucketNumber, uuids, columnIds, columnTypes, chunkColumnTypes );
        }
        catch (IOException | RuntimeException e) {
            storagePageSink.rollback();
            throw e;
        }

        updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());
        return shardInfos;
    }

    private List<ShardInfo> compact(StoragePageSink storagePageSink, OptionalInt bucketNumber, Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes, Map<Long, Type> chunkColumnTypes )
            throws IOException
    {
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes, chunkColumnTypes, Optional.empty())) {
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
        return getFutureValue(storagePageSink.commit());
    }

    public List<ShardInfo> compactSorted(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, CompressionType compressionType, List<SortOrder> sortOrders)
            throws IOException
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");

        long start = System.nanoTime();

        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());
        Map<Long, Type> chunkColumnTypes = columns.stream().collect(Collectors.toMap(ColumnInfo::getColumnId, ColumnInfo::getType));

        checkArgument(columnIds.containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");

        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        Queue<SortedRowSource> rowSources = new PriorityQueue<>();
        StoragePageSink outputPageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, false, compressionType);
        try {
            for (UUID uuid : uuids) {
                ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes, chunkColumnTypes, Optional.empty());
                SortedRowSource rowSource = new SortedRowSource(pageSource, columnTypes, sortIndexes, sortOrders);
                rowSources.add(rowSource);
            }
            while (!rowSources.isEmpty()) {
                ImmutableList.Builder<PageIndexInfo> pageIndexInfoBuilder = ImmutableList.builder();
                SortedRowSource rowSource = rowSources.poll();
                if (!rowSource.hasNext()) {
                    // rowSource is empty, close it
                    rowSource.close();
                    continue;
                }
                pageIndexInfoBuilder.add(rowSource.next());
                outputPageSink.appendPageIndexInfos(pageIndexInfoBuilder.build());
                if (outputPageSink.isFull()) {
                    outputPageSink.flush();
                }
                rowSources.add(rowSource);
            }
            outputPageSink.flush();
            List<ShardInfo> shardInfos = getFutureValue(outputPageSink.commit());

            updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());

            return shardInfos;
        }
        catch (IOException | RuntimeException e) {
            outputPageSink.rollback();
            throw e;
        }
        finally {
            rowSources.forEach(SortedRowSource::closeQuietly);
        }
    }

    public static boolean isNullOrEmptyPage(Page nextPage)
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
