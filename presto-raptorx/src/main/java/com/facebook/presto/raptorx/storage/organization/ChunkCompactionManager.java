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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.storage.StorageConfig;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.getOrganizationEligibleChunks;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ChunkCompactionManager
{
    private static final Logger log = Logger.get(ChunkCompactionManager.class);

    private static final double FILL_FACTOR = 0.75;

    private final ScheduledExecutorService compactionDiscoveryService = newScheduledThreadPool(1, daemonThreadsNamed("chunk-compaction-discovery"));

    private final AtomicBoolean discoveryStarted = new AtomicBoolean();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final ChunkOrganizer organizer;
    private final ChunkManager chunkManager;
    private final String currentNodeIdentifier;
    private final CompactionSetCreator compactionSetCreator;

    private final boolean compactionEnabled;
    private final Duration compactionDiscoveryInterval;
    private final DataSize maxChunkSize;
    private final long maxChunkRows;

    private final List<Jdbi> shardDbi;
    private Metadata metadata;

    @Inject
    public ChunkCompactionManager(Database database,
            NodeManager nodeManager,
            ChunkManager chunkManager,
            ChunkOrganizer organizer,
            TemporalFunction temporalFunction,
            StorageConfig config,
            Metadata metadata)
    {
        this(database,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                chunkManager,
                organizer,
                temporalFunction,
                metadata,
                config.getCompactionInterval(),
                config.getMaxChunkSize(),
                config.getMaxChunkRows(),
                config.isCompactionEnabled());
    }

    public ChunkCompactionManager(
            Database database,
            String currentNodeIdentifier,
            ChunkManager chunkManager,
            ChunkOrganizer organizer,
            TemporalFunction temporalFunction,
            Metadata metadata,
            Duration compactionDiscoveryInterval,
            DataSize maxChunkSize,
            long maxChunkRows,
            boolean compactionEnabled)
    {
        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.organizer = requireNonNull(organizer, "organizer is null");
        this.compactionDiscoveryInterval = requireNonNull(compactionDiscoveryInterval, "compactionDiscoveryInterval is null");

        checkArgument(maxChunkSize.toBytes() > 0, "maxChunkSize must be > 0");
        this.maxChunkSize = requireNonNull(maxChunkSize, "maxShardSize is null");

        checkArgument(maxChunkRows > 0, "maxChunkRows must be > 0");
        this.maxChunkRows = maxChunkRows;

        this.compactionEnabled = compactionEnabled;
        this.compactionSetCreator = new CompactionSetCreator(temporalFunction, maxChunkSize, maxChunkRows);
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
                discoverChunks();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error discovering chunks to compact");
            }
        }, 0, compactionDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void discoverChunks()
    {
        log.info("Discovering chunks that need compaction...");
        Set<ChunkMetadata> allChunks = chunkManager.getNodeChunkMetas(currentNodeIdentifier);
        ListMultimap<Long, ChunkMetadata> tableChunks = Multimaps.index(allChunks, ChunkMetadata::getTableId);

        for (Entry<Long, List<ChunkMetadata>> entry : Multimaps.asMap(tableChunks).entrySet()) {
            long tableId = entry.getKey();
            TableInfo tableInfo = metadata.getTableInfoForBgJob(tableId);
            List<ChunkMetadata> chunks = entry.getValue();
            Collection<OrganizationSet> organizationSets = filterAndCreateCompactionSets(tableInfo, chunks);
            log.info("Created %s organization set(s) for table ID %s", organizationSets.size(), tableId);

            for (OrganizationSet set : organizationSets) {
                organizer.enqueue(set);
            }
        }
    }

    private Collection<OrganizationSet> filterAndCreateCompactionSets(TableInfo tableInfo, Collection<ChunkMetadata> tableChunks)
    {
        OptionalLong temporalColumnId = tableInfo.getTemporalColumnId();
        if (temporalColumnId.isPresent()) {
            ColumnInfo tableColumn = tableInfo.getTemporalColumn().get();
            if (!isValidTemporalColumn(tableInfo.getTableId(), tableColumn.getType())) {
                return ImmutableSet.of();
            }
        }

        Set<ChunkMetadata> filteredChunks = tableChunks.stream()
                .filter(this::needsCompaction)
                .filter(chunk -> !organizer.inProgress(chunk.getChunkId()))
                .collect(toSet());

        Collection<ChunkIndexInfo> chunkIndexInfos = getOrganizationEligibleChunks(shardDbi, tableInfo, filteredChunks, false);
        if (tableInfo.getTemporalColumnId().isPresent()) {
            Set<ChunkIndexInfo> temporalChunks = chunkIndexInfos.stream()
                    .filter(chunk -> chunk.getTemporalRange().isPresent())
                    .collect(toSet());
            return compactionSetCreator.createCompactionSets(tableInfo, temporalChunks);
        }

        return compactionSetCreator.createCompactionSets(tableInfo, chunkIndexInfos);
    }

    private static boolean isValidTemporalColumn(long tableId, Type type)
    {
        if (!type.equals(DATE) && !type.equals(TIMESTAMP)) {
            log.warn("Temporal column type of table ID %s set incorrectly to %s", tableId, type);
            return false;
        }
        return true;
    }

    private boolean needsCompaction(ChunkMetadata chunk)
    {
        if (chunk.getUncompressedSize() < (FILL_FACTOR * maxChunkSize.toBytes())) {
            return true;
        }

        if (chunk.getRowCount() < (FILL_FACTOR * maxChunkRows)) {
            return true;
        }
        return false;
    }
}
