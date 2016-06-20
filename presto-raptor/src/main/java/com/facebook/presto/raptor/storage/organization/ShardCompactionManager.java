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
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
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

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.toShardIndexInfo;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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
    private final CompactionSetCreator compactionSetCreator;

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
        this.compactionSetCreator = new CompactionSetCreator(maxShardSize, maxShardRows);
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
        log.info("Discovering shards that need compaction...");
        Set<ShardMetadata> allShards = shardManager.getNodeShards(currentNodeIdentifier);
        ListMultimap<Long, ShardMetadata> tableShards = Multimaps.index(allShards, ShardMetadata::getTableId);

        for (Entry<Long, List<ShardMetadata>> entry : Multimaps.asMap(tableShards).entrySet()) {
            long tableId = entry.getKey();
            if (!metadataDao.isCompactionEnabled(tableId)) {
                continue;
            }
            List<ShardMetadata> shards = entry.getValue();
            Collection<OrganizationSet> organizationSets = filterAndCreateCompactionSets(tableId, shards);
            log.info("Created %s organization set(s) for table ID %s", organizationSets.size(), tableId);

            for (OrganizationSet set : organizationSets) {
                organizer.enqueue(set);
            }
        }
    }

    private Collection<OrganizationSet> filterAndCreateCompactionSets(long tableId, Collection<ShardMetadata> tableShards)
    {
        Table tableInfo = metadataDao.getTableInformation(tableId);
        OptionalLong temporalColumnId = tableInfo.getTemporalColumnId();
        if (temporalColumnId.isPresent()) {
            TableColumn tableColumn = metadataDao.getTableColumn(tableId, temporalColumnId.getAsLong());
            if (!isValidTemporalColumn(tableId, tableColumn.getDataType())) {
                return ImmutableSet.of();
            }
        }

        Set<ShardMetadata> filteredShards = tableShards.stream()
                .filter(this::needsCompaction)
                .filter(shard -> !organizer.inProgress(shard.getShardUuid()))
                .collect(toSet());

        Collection<ShardIndexInfo> shardIndexInfos = toShardIndexInfo(dbi, metadataDao, tableInfo, filteredShards, false);
        if (tableInfo.getTemporalColumnId().isPresent()) {
            Set<ShardIndexInfo> temporalShards = shardIndexInfos.stream()
                    .filter(shard -> shard.getTemporalRange().isPresent())
                    .collect(toSet());
            return compactionSetCreator.createCompactionSets(tableInfo, temporalShards);
        }

        return compactionSetCreator.createCompactionSets(tableInfo, shardIndexInfos);
    }

    private static boolean isValidTemporalColumn(long tableId, Type type)
    {
        if (!type.equals(DATE) && !type.equals(TIMESTAMP)) {
            log.warn("Temporal column type of table ID %s set incorrectly to %s", tableId, type);
            return false;
        }
        return true;
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
