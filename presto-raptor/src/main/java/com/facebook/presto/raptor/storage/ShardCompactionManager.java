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
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.TableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ShardCompactionManager
{
    private static final double FILL_FACTOR = 0.75;
    private static final Logger log = Logger.get(ShardCompactionManager.class);

    private final ScheduledExecutorService compactionDiscoveryService = newScheduledThreadPool(1, daemonThreadsNamed("shard-compaction-discovery"));
    private final ExecutorService compactionDriverService = newFixedThreadPool(1, daemonThreadsNamed("shard-compaction-driver"));
    private final ExecutorService compactionService;

    private final AtomicBoolean discoveryStarted = new AtomicBoolean();
    private final AtomicBoolean compactionStarted = new AtomicBoolean();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    // Tracks shards that are scheduled for compaction so that we do not schedule them more than once
    private final Set<Long> shardsBeingCompacted = newConcurrentHashSet();
    private final BlockingQueue<CompactionSet> compactionQueue = new LinkedBlockingQueue<>();

    private final MetadataDao metadataDao;
    private final ShardCompactor compactor;
    private final ShardManager shardManager;
    private final String currentNodeIdentifier;

    private final Duration compactionDiscoveryInterval;
    private final DataSize maxShardSize;
    private final long maxShardRows;

    @Inject
    public ShardCompactionManager(@ForMetadata IDBI dbi, NodeManager nodeManager, ShardManager shardManager, ShardCompactor compactor, StorageManagerConfig config)
    {
        this(dbi,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                shardManager,
                compactor,
                config.getCompactionInterval(),
                config.getMaxShardSize(),
                config.getMaxShardRows(),
                config.getCompactionThreads());
    }

    public ShardCompactionManager(
            IDBI dbi,
            String currentNodeIdentifier,
            ShardManager shardManager,
            ShardCompactor compactor,
            Duration compactionDiscoveryInterval,
            DataSize maxShardSize,
            long maxShardRows,
            int compactionThreads)
    {
        requireNonNull(dbi, "dbi is null");
        this.metadataDao = dbi.onDemand(MetadataDao.class);

        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.compactionDiscoveryInterval = requireNonNull(compactionDiscoveryInterval, "compactionDiscoveryInterval is null");

        checkArgument(maxShardSize.toBytes() > 0, "maxShardSize must be > 0");
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;

        checkArgument(compactionThreads > 0, "compactionThreads must be > 0");
        this.compactionService = newFixedThreadPool(compactionThreads, daemonThreadsNamed("shard-compactor-%s"));
    }

    @PostConstruct
    public void start()
    {
        if (!discoveryStarted.getAndSet(true)) {
            startDiscovery();
        }

        if (compactionStarted.getAndSet(true)) {
            compactionDriverService.submit(new ShardCompactionDriver());
        }
    }

    @PreDestroy
    public void shutdown()
    {
        shutdown.set(true);
        compactionDiscoveryService.shutdown();
        compactionDriverService.shutdown();
        compactionService.shutdown();
    }

    private void startDiscovery()
    {
        compactionDiscoveryService.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                SECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 30));
                discoverShards();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error discovering shards to compact");
            }
        }, 0, compactionDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void discoverShards()
    {
        CompactionSetCreator compactionSetCreator = new CompactionSetCreator(maxShardSize);

        for (long tableId : metadataDao.listTableIds()) {
            Set<ShardMetadata> shardMetadata = shardManager.getNodeTableShards(currentNodeIdentifier, tableId);
            Set<ShardMetadata> shards = shardMetadata.stream()
                    .filter(this::needsCompaction)
                    .filter(shard -> !shardsBeingCompacted.contains(shard.getShardId()))
                    .collect(toSet());
            if (shards.size() <= 1) {
                continue;
            }

            Long temporalColumnId = metadataDao.getTemporalColumnId(tableId);
            if (temporalColumnId != null) {
                // TODO implement time range aware compaction
            }
            else {
                addToCompactionQueue(compactionSetCreator, tableId, shards);
            }
        }
    }

    private void addToCompactionQueue(CompactionSetCreator compactionSetCreator, long tableId, Set<ShardMetadata> shardsToCompact)
    {
        for (Set<ShardMetadata> compactionSet : compactionSetCreator.getCompactionSets(shardsToCompact)) {
            if (compactionSet.size() <= 1) {
                // throw it away because there is no work to be done
                continue;
            }

            compactionSet.stream()
                    .map(ShardMetadata::getShardId)
                    .forEach(shardsBeingCompacted::add);

            compactionQueue.add(new CompactionSet(tableId, compactionSet));
        }
    }

    private boolean needsCompaction(ShardMetadata shard)
    {
        if (shard.getUncompressedSize() < (FILL_FACTOR * maxShardSize.toBytes())) {
            return true;
        }

        if (shard.getRowCount() < (FILL_FACTOR * maxShardRows)) {
            return true;
        }
        return false;
    }

    private class CompactionSet
    {
        private final long tableId;
        private final Set<ShardMetadata> shardsToCompact;

        public CompactionSet(long tableId, Set<ShardMetadata> shardsToCompact)
        {
            this.tableId = tableId;
            this.shardsToCompact = requireNonNull(shardsToCompact, "shardsToCompact is null");
        }

        public long getTableId()
        {
            return tableId;
        }

        public Set<ShardMetadata> getShardsToCompact()
        {
            return shardsToCompact;
        }
    }

    private class ShardCompactionDriver
            implements Runnable
    {
        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted() && !shutdown.get()) {
                try {
                    CompactionSet compactionSet = compactionQueue.take();
                    compactionService.submit(new CompactionJob(compactionSet));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private class CompactionJob
            implements Runnable
    {
        private final CompactionSet compactionSet;

        public CompactionJob(CompactionSet compactionSet)
        {
            this.compactionSet = requireNonNull(compactionSet, "compactionSet is null");
        }

        @Override
        public void run()
        {
            Set<UUID> shardUuids = compactionSet.getShardsToCompact().stream().map(ShardMetadata::getShardUuid).collect(toSet());
            Set<Long> shardIds = compactionSet.getShardsToCompact().stream().map(ShardMetadata::getShardId).collect(toSet());

            try {
                TableMetadata tableMetadata = getTableMetadata(compactionSet.getTableId());
                List<ShardInfo> newShards = performCompaction(shardUuids, tableMetadata);
                shardManager.replaceShards(tableMetadata.getTableId(), tableMetadata.getColumns(), shardIds, newShards);
                shardsBeingCompacted.removeAll(shardIds);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                shardsBeingCompacted.removeAll(shardIds);
            }
        }

        private List<ShardInfo> performCompaction(Set<UUID> shardUuids, TableMetadata tableMetadata)
                throws IOException
        {
            if (tableMetadata.getSortColumnIds().isEmpty()) {
                return compactor.compact(shardUuids, tableMetadata.getColumns());
            }
            return compactor.compactSorted(
                    shardUuids,
                    tableMetadata.getColumns(),
                    tableMetadata.getSortColumnIds(),
                    nCopies(tableMetadata.getSortColumnIds().size(), ASC_NULLS_FIRST));
        }
    }

    private TableMetadata getTableMetadata(long tableId)
    {
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        List<Long> sortColumnIds = sortColumns.stream().map(TableColumn::getColumnId).collect(toList());
        List<ColumnInfo> columns = metadataDao.listTableColumns(tableId).stream()
                .map(TableColumn::toColumnInfo)
                .collect(toList());
        return new TableMetadata(tableId, columns, sortColumnIds);

    }
}
