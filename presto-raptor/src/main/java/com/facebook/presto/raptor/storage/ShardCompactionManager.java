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
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.maxColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.minColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ShardCompactionManager
{
    private static final Logger log = Logger.get(ShardCompactionManager.class);

    private static final double FILL_FACTOR = 0.75;
    private static final int MAX_PENDING_COMPACTIONS = 500;

    private final ScheduledExecutorService compactionDiscoveryService = newScheduledThreadPool(1, daemonThreadsNamed("shard-compaction-discovery"));
    private final ExecutorService compactionDriverService = newFixedThreadPool(1, daemonThreadsNamed("shard-compaction-driver"));
    private final ExecutorService compactionService;

    private final AtomicBoolean discoveryStarted = new AtomicBoolean();
    private final AtomicBoolean compactionStarted = new AtomicBoolean();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    // Tracks shards that are scheduled for compaction so that we do not schedule them more than once
    private final Set<UUID> shardsInProgress = newConcurrentHashSet();
    private final BlockingQueue<CompactionSet> compactionQueue = new LinkedBlockingQueue<>();

    private final MetadataDao metadataDao;
    private final ShardCompactor compactor;
    private final ShardManager shardManager;
    private final String currentNodeIdentifier;

    private final boolean compactionEnabled;
    private final Duration compactionDiscoveryInterval;
    private final DataSize maxShardSize;
    private final long maxShardRows;
    private final IDBI dbi;

    private final CounterStat compactionSuccessCount = new CounterStat();
    private final CounterStat compactionFailureCount = new CounterStat();

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
                config.getCompactionThreads(),
                config.isCompactionEnabled());
    }

    public ShardCompactionManager(
            IDBI dbi,
            String currentNodeIdentifier,
            ShardManager shardManager,
            ShardCompactor compactor,
            Duration compactionDiscoveryInterval,
            DataSize maxShardSize,
            long maxShardRows,
            int compactionThreads,
            boolean compactionEnabled)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);

        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.compactionDiscoveryInterval = requireNonNull(compactionDiscoveryInterval, "compactionDiscoveryInterval is null");

        checkArgument(maxShardSize.toBytes() > 0, "maxShardSize must be > 0");
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;

        this.compactionEnabled = compactionEnabled;
        if (compactionEnabled) {
            checkArgument(compactionThreads > 0, "compactionThreads must be > 0");
            this.compactionService = newFixedThreadPool(compactionThreads, daemonThreadsNamed("shard-compactor-%s"));
        }
        else {
            this.compactionService = null;
        }
    }

    @PostConstruct
    public void start()
    {
        if (!compactionEnabled) {
            return;
        }

        if (!discoveryStarted.getAndSet(true)) {
            startDiscovery();
        }

        if (!compactionStarted.getAndSet(true)) {
            compactionDriverService.submit(new ShardCompactionDriver());
        }
    }

    @PreDestroy
    public void shutdown()
    {
        if (!compactionEnabled) {
            return;
        }
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
                long interval = (long) compactionDiscoveryInterval.convertTo(SECONDS).getValue();
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
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
        if (shardsInProgress.size() >= MAX_PENDING_COMPACTIONS) {
            return;
        }

        Set<ShardMetadata> allShards = shardManager.getNodeShards(currentNodeIdentifier);
        ListMultimap<Long, ShardMetadata> tableShards = Multimaps.index(allShards, ShardMetadata::getTableId);

        for (Entry<Long, List<ShardMetadata>> entry : Multimaps.asMap(tableShards).entrySet()) {
            long tableId = entry.getKey();
            if (!metadataDao.isCompactionEnabled(tableId)) {
                continue;
            }

            Optional<Long> temporalColumnId = Optional.ofNullable(metadataDao.getTemporalColumnId(tableId));
            if (temporalColumnId.isPresent() && !isValidTemporalColumn(tableId, temporalColumnId.get())) {
                continue;
            }

            CompactionSetCreator compactionSetCreator = getCompactionSetCreator(tableId, temporalColumnId);
            Set<ShardMetadata> shards = getFilteredShards(entry.getValue(), tableId, temporalColumnId);

            addToCompactionQueue(compactionSetCreator, tableId, shards);
        }
    }

    private Set<ShardMetadata> getFilteredShards(List<ShardMetadata> shardMetadatas, long tableId, Optional<Long> temporalColumnId)
    {
        Set<ShardMetadata> shards = shardMetadatas.stream()
                .filter(this::needsCompaction)
                .filter(shard -> !shardsInProgress.contains(shard.getShardUuid()))
                .collect(toSet());

        if (temporalColumnId.isPresent()) {
            shards = filterShardsWithTemporalMetadata(shards, tableId, temporalColumnId.get());
        }

        return shards;
    }

    private CompactionSetCreator getCompactionSetCreator(long tableId, Optional<Long> temporalColumnId)
    {
        if (!temporalColumnId.isPresent()) {
            return new FileCompactionSetCreator(maxShardSize, maxShardRows);
        }

        checkState(isValidTemporalColumn(tableId, temporalColumnId.get()), "invalid temporal column type");
        Type type = metadataDao.getTableColumn(tableId, temporalColumnId.get()).getDataType();

        return new TemporalCompactionSetCreator(maxShardSize, maxShardRows, type);
    }

    private boolean isValidTemporalColumn(long tableId, long temporalColumnId)
    {
        Type type = metadataDao.getTableColumn(tableId, temporalColumnId).getDataType();
        if (!type.equals(DATE) && !type.equals(TIMESTAMP)) {
            log.warn("Temporal column type of table ID %s set incorrectly to %s", tableId, type);
            return false;
        }
        return true;
    }

    /**
     * @return shards that have temporal information
     */
    @VisibleForTesting
    Set<ShardMetadata> filterShardsWithTemporalMetadata(Iterable<ShardMetadata> allShards, long tableId, long temporalColumnId)
    {
        Map<Long, ShardMetadata> shardsById = uniqueIndex(allShards, ShardMetadata::getShardId);

        String minColumn = minColumn(temporalColumnId);
        String maxColumn = maxColumn(temporalColumnId);

        ImmutableSet.Builder<ShardMetadata> temporalShards = ImmutableSet.builder();
        try (Connection connection = dbi.open().getConnection()) {
            for (List<ShardMetadata> shards : partition(allShards, 1000)) {
                String args = Joiner.on(",").join(nCopies(shards.size(), "?"));
                String sql = format("SELECT shard_id, %s, %s FROM %s WHERE shard_id IN (%s)",
                        minColumn, maxColumn, shardIndexTable(tableId), args);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < shards.size(); i++) {
                        statement.setLong(i + 1, shards.get(i).getShardId());
                    }

                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            long rangeStart = resultSet.getLong(minColumn);
                            if (resultSet.wasNull()) {
                                // no temporal information for shard, skip it
                                continue;
                            }
                            long rangeEnd = resultSet.getLong(maxColumn);
                            if (resultSet.wasNull()) {
                                // no temporal information for shard, skip it
                                continue;
                            }
                            long shardId = resultSet.getLong("shard_id");
                            if (resultSet.wasNull()) {
                                // shard does not exist anymore
                                continue;
                            }
                            ShardMetadata shard = shardsById.get(shardId);
                            if (shard != null) {
                                temporalShards.add(shard.withTimeRange(rangeStart, rangeEnd));
                            }
                        }
                    }
                }
            }
        }
        catch (SQLException e) {
            throw metadataError(e);
        }
        return temporalShards.build();
    }

    private void addToCompactionQueue(CompactionSetCreator compactionSetCreator, long tableId, Set<ShardMetadata> shardsToCompact)
    {
        for (CompactionSet compactionSet : compactionSetCreator.createCompactionSets(tableId, shardsToCompact)) {
            if (compactionSet.getShardsToCompact().size() <= 1) {
                // throw it away because there is no work to be done
                continue;
            }

            compactionSet.getShardsToCompact().stream()
                    .map(ShardMetadata::getShardUuid)
                    .forEach(shardsInProgress::add);

            compactionQueue.add(compactionSet);
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

    private class ShardCompactionDriver
            implements Runnable
    {
        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted() && !shutdown.get()) {
                try {
                    CompactionSet compactionSet = compactionQueue.take();
                    runAsync(new CompactionJob(compactionSet), compactionService)
                            .whenComplete((none, throwable) -> {
                                if (throwable == null) {
                                    compactionSuccessCount.update(1);
                                }
                                else {
                                    log.warn(throwable, "Error in compaction");
                                    compactionFailureCount.update(1);
                                }
                            });
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
            Set<ShardMetadata> shards = compactionSet.getShardsToCompact();
            OptionalInt bucketNumber = shards.iterator().next().getBucketNumber();
            for (ShardMetadata shard : shards) {
                verify(bucketNumber.equals(shard.getBucketNumber()), "mismatched bucket numbers");
            }
            Set<UUID> shardUuids = shards.stream().map(ShardMetadata::getShardUuid).collect(toSet());

            try {
                compactShards(compactionSet.getTableId(), bucketNumber, shardUuids);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                shardsInProgress.removeAll(shardUuids);
            }
        }

        private void compactShards(long tableId, OptionalInt bucketNumber, Set<UUID> shardUuids)
                throws IOException
        {
            long transactionId = shardManager.beginTransaction();
            try {
                compactShards(transactionId, bucketNumber, tableId, shardUuids);
            }
            catch (Throwable e) {
                shardManager.rollbackTransaction(transactionId);
                throw e;
            }
        }

        private void compactShards(long transactionId, OptionalInt bucketNumber, long tableId, Set<UUID> shardUuids)
                throws IOException
        {
            TableMetadata metadata = getTableMetadata(tableId);
            List<ShardInfo> newShards = performCompaction(transactionId, bucketNumber, shardUuids, metadata);
            log.info("Compacted shards %s into %s", shardUuids, newShards.stream().map(ShardInfo::getShardUuid).collect(toList()));
            shardManager.replaceShardUuids(transactionId, tableId, metadata.getColumns(), shardUuids, newShards);
        }

        private List<ShardInfo> performCompaction(long transactionId, OptionalInt bucketNumber, Set<UUID> shardUuids, TableMetadata tableMetadata)
                throws IOException
        {
            if (tableMetadata.getSortColumnIds().isEmpty()) {
                return compactor.compact(transactionId, bucketNumber, shardUuids, tableMetadata.getColumns());
            }
            return compactor.compactSorted(
                    transactionId,
                    bucketNumber,
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

    @Managed
    public int getShardsInProgress()
    {
        return shardsInProgress.size();
    }

    @Managed
    @Nested
    public CounterStat getCompactionSuccessCount()
    {
        return compactionSuccessCount;
    }

    @Managed
    @Nested
    public CounterStat getCompactionFailureCount()
    {
        return compactionFailureCount;
    }
}
