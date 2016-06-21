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

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.maxColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.minColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ShardCompactionManager
{
    private static final Logger log = Logger.get(ShardCompactionManager.class);

    private static final double FILL_FACTOR = 0.75;

    private final ScheduledExecutorService compactionDiscoveryService = newScheduledThreadPool(1, daemonThreadsNamed("shard-compaction-discovery"));

    private final AtomicBoolean discoveryStarted = new AtomicBoolean();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final MetadataDao metadataDao;
    private final ShardOrganizer organizer;
    private final ShardManager shardManager;
    private final String currentNodeIdentifier;

    private final boolean compactionEnabled;
    private final Duration compactionDiscoveryInterval;
    private final DataSize maxShardSize;
    private final long maxShardRows;
    private final IDBI dbi;

    @Inject
    public ShardCompactionManager(@ForMetadata IDBI dbi, NodeManager nodeManager, ShardManager shardManager, ShardOrganizer organizer, StorageManagerConfig config)
    {
        this(dbi,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                shardManager,
                organizer,
                config.getCompactionInterval(),
                config.getMaxShardSize(),
                config.getMaxShardRows(),
                config.isCompactionEnabled());
    }

    public ShardCompactionManager(
            IDBI dbi,
            String currentNodeIdentifier,
            ShardManager shardManager,
            ShardOrganizer organizer,
            Duration compactionDiscoveryInterval,
            DataSize maxShardSize,
            long maxShardRows,
            boolean compactionEnabled)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);

        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.organizer = requireNonNull(organizer, "organizer is null");
        this.compactionDiscoveryInterval = requireNonNull(compactionDiscoveryInterval, "compactionDiscoveryInterval is null");

        checkArgument(maxShardSize.toBytes() > 0, "maxShardSize must be > 0");
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;

        this.compactionEnabled = compactionEnabled;
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
    }

    @PreDestroy
    public void shutdown()
    {
        if (!compactionEnabled) {
            return;
        }
        shutdown.set(true);
        compactionDiscoveryService.shutdown();
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
        Set<ShardMetadata> allShards = shardManager.getNodeShards(currentNodeIdentifier);
        ListMultimap<Long, ShardMetadata> tableShards = Multimaps.index(allShards, ShardMetadata::getTableId);

        for (Entry<Long, List<ShardMetadata>> entry : Multimaps.asMap(tableShards).entrySet()) {
            long tableId = entry.getKey();
            if (!metadataDao.isCompactionEnabled(tableId)) {
                continue;
            }

            Table table = metadataDao.getTableInformation(tableId);

            CompactionSetCreator compactionSetCreator = getCompactionSetCreator(table);
            Set<ShardMetadata> shards = getFilteredShards(entry.getValue(), table);

            addToCompactionQueue(compactionSetCreator, tableId, shards);
        }
    }

    private Set<ShardMetadata> getFilteredShards(List<ShardMetadata> shardMetadatas, Table tableInfo)
    {
        Set<ShardMetadata> shards = shardMetadatas.stream()
                .filter(this::needsCompaction)
                .filter(shard -> !organizer.inProgress(shard.getShardUuid()))
                .collect(toSet());

        if (tableInfo.getTemporalColumnId().isPresent()) {
            shards = filterShardsWithTemporalMetadata(shards, tableInfo.getTableId(), tableInfo.getTemporalColumnId().getAsLong());
        }

        return shards;
    }

    private CompactionSetCreator getCompactionSetCreator(Table tableInfo)
    {
        if (!tableInfo.getTemporalColumnId().isPresent()) {
            return new FileCompactionSetCreator(maxShardSize, maxShardRows);
        }

        Type type = metadataDao.getTableColumn(tableInfo.getTableId(), tableInfo.getTemporalColumnId().getAsLong()).getDataType();
        verify(isValidTemporalColumn(tableInfo.getTableId(), type), "invalid temporal column type");
        return new TemporalCompactionSetCreator(maxShardSize, maxShardRows, type);
    }

    private static boolean isValidTemporalColumn(long tableId, Type type)
    {
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
        for (OrganizationSet compactionSet : compactionSetCreator.createCompactionSets(tableId, shardsToCompact)) {
            if (compactionSet.getShards().size() <= 1) {
                // throw it away because there is no work to be done
                continue;
            }

            organizer.enqueue(compactionSet);
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
}
