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
import com.facebook.presto.raptorx.metadata.MasterReaderDao;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.storage.StorageConfig;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.createOrganizationSet;
import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.getChunksByDaysBuckets;
import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.getOrganizationEligibleChunks;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.MoreFutures.allAsList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ChunkOrganizationManager
{
    private static final Logger log = Logger.get(ChunkOrganizationManager.class);

    private final ScheduledExecutorService discoveryService = newScheduledThreadPool(1, daemonThreadsNamed("chunk-organization-discovery"));
    private final AtomicBoolean started = new AtomicBoolean();

    private final ChunkManager chunkManager;
    private final ChunkOrganizerDao organizerDao;
    private final List<Jdbi> shardDbi;
    private final MasterReaderDao masterReaderDao;

    private final TemporalFunction temporalFunction;

    private final boolean enabled;
    private final long organizationIntervalMillis;
    private final long organizationDiscoveryIntervalMillis;

    private final String currentNodeIdentifier;
    private final long currentNodeId;
    private final ChunkOrganizer organizer;

    private final Set<Long> tablesInProgress = newConcurrentHashSet();

    @Inject
    public ChunkOrganizationManager(
            Database database,
            NodeIdCache nodeIdCache,
            NodeManager nodeManager,
            ChunkManager chunkManager,
            ChunkOrganizer organizer,
            TemporalFunction temporalFunction,
            StorageConfig config)
    {
        this(database,
                nodeIdCache,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                chunkManager,
                organizer,
                temporalFunction,
                config.isOrganizationEnabled(),
                config.getOrganizationInterval(),
                config.getOrganizationDiscoveryInterval());
    }

    public ChunkOrganizationManager(
            Database database,
            NodeIdCache nodeIdCache,
            String currentNodeIdentifier,
            ChunkManager chunkManager,
            ChunkOrganizer organizer,
            TemporalFunction temporalFunction,
            boolean enabled,
            Duration organizationInterval,
            Duration organizationDiscoveryInterval)
    {
        this.masterReaderDao = createJdbi(database.getMasterConnection()).onDemand(MasterReaderDao.class);
        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
        this.organizerDao = createJdbi(database.getMasterConnection()).onDemand(ChunkOrganizerDao.class);

        requireNonNull(nodeIdCache, "nodeIdCache is null");
        this.organizer = requireNonNull(organizer, "organizer is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.currentNodeId = nodeIdCache.getNodeId(currentNodeIdentifier);

        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        this.enabled = enabled;
        requireNonNull(organizationInterval, "organizationInterval is null");
        this.organizationIntervalMillis = max(1, organizationInterval.roundTo(MILLISECONDS));
        this.organizationDiscoveryIntervalMillis = max(1, organizationDiscoveryInterval.roundTo(MILLISECONDS));
    }

    @PostConstruct
    public void start()
    {
        if (!enabled || started.getAndSet(true)) {
            return;
        }

        startDiscovery();
    }

    @PreDestroy
    public void shutdown()
    {
        discoveryService.shutdownNow();
    }

    private void startDiscovery()
    {
        discoveryService.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database and overloading the backup store
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, organizationDiscoveryIntervalMillis));

                log.info("Running chunk organizer...");
                submitJobs(discoverAndInitializeTablesToOrganize());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error running chunk organizer");
            }
        }, 0, organizationDiscoveryIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    Set<Long> discoverAndInitializeTablesToOrganize()
    {
        Set<Long> enabledTableIds = masterReaderDao.getOrganizationEligibleTables();

        Set<TableOrganizationInfo> tableOrganizationInfo = organizerDao.getNodeTableOrganizationInfo(currentNodeId);
        Map<Long, TableOrganizationInfo> organizationInfos = Maps.uniqueIndex(tableOrganizationInfo, TableOrganizationInfo::getTableId);

        // If this is the first time organizing a table, initialize the organization info for it
        difference(enabledTableIds, organizationInfos.keySet())
                .forEach(tableId -> organizerDao.insertNode(currentNodeId, tableId));

        ImmutableSet.Builder<Long> tableIds = ImmutableSet.builder();
        for (Long tableId : enabledTableIds) {
            TableOrganizationInfo info = organizationInfos.get(tableId);
            if (info == null || shouldRunOrganization(info)) {
                tableIds.add(tableId);
            }
        }
        return tableIds.build();
    }

    private void submitJobs(Set<Long> tableIds)
    {
        tableIds.forEach(this::runOrganization);
    }

    private void runOrganization(long tableId)
    {
        Set<ChunkMetadata> chunkMetadatas = chunkManager.getNodeChunkMetas(currentNodeIdentifier);
        TableInfo tableInfo = masterReaderDao.getTableInfo(tableId);
        Set<ChunkMetadata> filteredChunks = chunkMetadatas.stream()
                .filter(chunk -> !organizer.inProgress(chunk.getChunkId()))
                .collect(toSet());

        Collection<ChunkIndexInfo> indexInfos = getOrganizationEligibleChunks(shardDbi, tableInfo, filteredChunks, true);
        Set<OrganizationSet> organizationSets = createOrganizationSets(temporalFunction, tableInfo, indexInfos);

        if (organizationSets.isEmpty()) {
            return;
        }

        log.info("Created %s organization set(s) from %s chunks for table ID %s", organizationSets.size(), filteredChunks.size(), tableId);

        long lastStartTime = System.currentTimeMillis();
        tablesInProgress.add(tableId);

        ImmutableList.Builder<CompletableFuture<?>> futures = ImmutableList.builder();
        for (OrganizationSet organizationSet : organizationSets) {
            futures.add(organizer.enqueue(organizationSet));
        }
        allAsList(futures.build())
                .whenComplete((value, throwable) -> {
                    tablesInProgress.remove(tableId);
                    organizerDao.updateLastStartTime(currentNodeId, tableId, lastStartTime);
                });
    }

    private boolean shouldRunOrganization(TableOrganizationInfo info)
    {
        // skip if organization is in progress for this table
        if (tablesInProgress.contains(info.getTableId())) {
            return false;
        }

        if (!info.getLastStartTimeMillis().isPresent()) {
            return true;
        }

        return (System.currentTimeMillis() - info.getLastStartTimeMillis().getAsLong()) >= organizationIntervalMillis;
    }

    @VisibleForTesting
    static Set<OrganizationSet> createOrganizationSets(TemporalFunction temporalFunction, TableInfo tableInfo, Collection<ChunkIndexInfo> chunks)
    {
        return getChunksByDaysBuckets(tableInfo, chunks, temporalFunction).stream()
                .map(indexInfos -> getOverlappingOrganizationSets(tableInfo, indexInfos))
                .flatMap(Collection::stream)
                .collect(toSet());
    }

    private static Set<OrganizationSet> getOverlappingOrganizationSets(TableInfo tableInfo, Collection<ChunkIndexInfo> chunks)
    {
        if (chunks.size() <= 1) {
            return ImmutableSet.of();
        }

        // Sort by low marker for the range
        List<ChunkIndexInfo> sortedChunks = chunks.stream()
                .sorted((o1, o2) -> {
                    ChunkRange sortRange1 = o1.getSortRange().get();
                    ChunkRange sortRange2 = o2.getSortRange().get();
                    return ComparisonChain.start()
                            .compare(sortRange1.getMinTuple(), sortRange2.getMinTuple())
                            .compare(sortRange2.getMaxTuple(), sortRange1.getMaxTuple())
                            .result();
                })
                .collect(toList());

        Set<OrganizationSet> organizationSets = new HashSet<>();
        ImmutableSet.Builder<ChunkIndexInfo> builder = ImmutableSet.builder();

        builder.add(sortedChunks.get(0));
        int previousRange = 0;
        int nextRange = previousRange + 1;
        while (nextRange < sortedChunks.size()) {
            ChunkRange sortRange1 = sortedChunks.get(previousRange).getSortRange().get();
            ChunkRange sortRange2 = sortedChunks.get(nextRange).getSortRange().get();

            if (sortRange1.overlaps(sortRange2) && !sortRange1.adjacent(sortRange2)) {
                builder.add(sortedChunks.get(nextRange));
                if (!sortRange1.encloses(sortRange2)) {
                    previousRange = nextRange;
                }
            }
            else {
                Set<ChunkIndexInfo> indexInfos = builder.build();
                if (indexInfos.size() > 1) {
                    organizationSets.add(createOrganizationSet(tableInfo.getTableId(), indexInfos));
                }
                builder = ImmutableSet.builder();
                previousRange = nextRange;
                builder.add(sortedChunks.get(previousRange));
            }
            nextRange++;
        }

        Set<ChunkIndexInfo> indexInfos = builder.build();
        if (indexInfos.size() > 1) {
            organizationSets.add(createOrganizationSet(tableInfo.getTableId(), indexInfos));
        }
        return organizationSets;
    }
}
