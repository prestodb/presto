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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.raptor.legacy.metadata.ForMetadata;
import io.prestosql.plugin.raptor.legacy.metadata.MetadataDao;
import io.prestosql.plugin.raptor.legacy.metadata.ShardManager;
import io.prestosql.plugin.raptor.legacy.metadata.ShardMetadata;
import io.prestosql.plugin.raptor.legacy.metadata.Table;
import io.prestosql.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.prestosql.spi.NodeManager;
import org.skife.jdbi.v2.IDBI;

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

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.MoreFutures.allAsList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.createOrganizationSet;
import static io.prestosql.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.getOrganizationEligibleShards;
import static io.prestosql.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.getShardsByDaysBuckets;
import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.onDemandDao;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ShardOrganizationManager
{
    private static final Logger log = Logger.get(ShardOrganizationManager.class);

    private final ScheduledExecutorService discoveryService = newScheduledThreadPool(1, daemonThreadsNamed("shard-organization-discovery"));
    private final AtomicBoolean started = new AtomicBoolean();

    private final IDBI dbi;
    private final MetadataDao metadataDao;
    private final ShardOrganizerDao organizerDao;
    private final ShardManager shardManager;
    private final TemporalFunction temporalFunction;

    private final boolean enabled;
    private final long organizationIntervalMillis;
    private final long organizationDiscoveryIntervalMillis;

    private final String currentNodeIdentifier;
    private final ShardOrganizer organizer;

    private final Set<Long> tablesInProgress = newConcurrentHashSet();

    @Inject
    public ShardOrganizationManager(
            @ForMetadata IDBI dbi,
            NodeManager nodeManager,
            ShardManager shardManager,
            ShardOrganizer organizer,
            TemporalFunction temporalFunction,
            StorageManagerConfig config)
    {
        this(dbi,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                shardManager,
                organizer,
                temporalFunction,
                config.isOrganizationEnabled(),
                config.getOrganizationInterval(),
                config.getOrganizationDiscoveryInterval());
    }

    public ShardOrganizationManager(
            IDBI dbi,
            String currentNodeIdentifier,
            ShardManager shardManager,
            ShardOrganizer organizer,
            TemporalFunction temporalFunction,
            boolean enabled,
            Duration organizationInterval,
            Duration organizationDiscoveryInterval)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.organizerDao = onDemandDao(dbi, ShardOrganizerDao.class);

        this.organizer = requireNonNull(organizer, "organizer is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
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

                log.info("Running shard organizer...");
                submitJobs(discoverAndInitializeTablesToOrganize());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error running shard organizer");
            }
        }, 0, organizationDiscoveryIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    Set<Long> discoverAndInitializeTablesToOrganize()
    {
        Set<Long> enabledTableIds = metadataDao.getOrganizationEligibleTables();

        Set<TableOrganizationInfo> tableOrganizationInfo = organizerDao.getNodeTableOrganizationInfo(currentNodeIdentifier);
        Map<Long, TableOrganizationInfo> organizationInfos = Maps.uniqueIndex(tableOrganizationInfo, TableOrganizationInfo::getTableId);

        // If this is the first time organizing a table, initialize the organization info for it
        difference(enabledTableIds, organizationInfos.keySet())
                .forEach(tableId -> organizerDao.insertNode(currentNodeIdentifier, tableId));

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
        Set<ShardMetadata> shardMetadatas = shardManager.getNodeShards(currentNodeIdentifier, tableId);
        Table tableInfo = metadataDao.getTableInformation(tableId);
        Set<ShardMetadata> filteredShards = shardMetadatas.stream()
                .filter(shard -> !organizer.inProgress(shard.getShardUuid()))
                .collect(toSet());

        Collection<ShardIndexInfo> indexInfos = getOrganizationEligibleShards(dbi, metadataDao, tableInfo, filteredShards, true);
        Set<OrganizationSet> organizationSets = createOrganizationSets(temporalFunction, tableInfo, indexInfos);

        if (organizationSets.isEmpty()) {
            return;
        }

        log.info("Created %s organization set(s) from %s shards for table ID %s", organizationSets.size(), filteredShards.size(), tableId);

        long lastStartTime = System.currentTimeMillis();
        tablesInProgress.add(tableId);

        ImmutableList.Builder<CompletableFuture<?>> futures = ImmutableList.builder();
        for (OrganizationSet organizationSet : organizationSets) {
            futures.add(organizer.enqueue(organizationSet));
        }
        allAsList(futures.build())
                .whenComplete((value, throwable) -> {
                    tablesInProgress.remove(tableId);
                    organizerDao.updateLastStartTime(currentNodeIdentifier, tableId, lastStartTime);
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
    static Set<OrganizationSet> createOrganizationSets(TemporalFunction temporalFunction, Table tableInfo, Collection<ShardIndexInfo> shards)
    {
        return getShardsByDaysBuckets(tableInfo, shards, temporalFunction).stream()
                .map(indexInfos -> getOverlappingOrganizationSets(tableInfo, indexInfos))
                .flatMap(Collection::stream)
                .collect(toSet());
    }

    private static Set<OrganizationSet> getOverlappingOrganizationSets(Table tableInfo, Collection<ShardIndexInfo> shards)
    {
        if (shards.size() <= 1) {
            return ImmutableSet.of();
        }

        // Sort by low marker for the range
        List<ShardIndexInfo> sortedShards = shards.stream()
                .sorted((o1, o2) -> {
                    ShardRange sortRange1 = o1.getSortRange().get();
                    ShardRange sortRange2 = o2.getSortRange().get();
                    return ComparisonChain.start()
                            .compare(sortRange1.getMinTuple(), sortRange2.getMinTuple())
                            .compare(sortRange2.getMaxTuple(), sortRange1.getMaxTuple())
                            .result();
                })
                .collect(toList());

        Set<OrganizationSet> organizationSets = new HashSet<>();
        ImmutableSet.Builder<ShardIndexInfo> builder = ImmutableSet.builder();

        builder.add(sortedShards.get(0));
        int previousRange = 0;
        int nextRange = previousRange + 1;
        while (nextRange < sortedShards.size()) {
            ShardRange sortRange1 = sortedShards.get(previousRange).getSortRange().get();
            ShardRange sortRange2 = sortedShards.get(nextRange).getSortRange().get();

            if (sortRange1.overlaps(sortRange2) && !sortRange1.adjacent(sortRange2)) {
                builder.add(sortedShards.get(nextRange));
                if (!sortRange1.encloses(sortRange2)) {
                    previousRange = nextRange;
                }
            }
            else {
                Set<ShardIndexInfo> indexInfos = builder.build();
                if (indexInfos.size() > 1) {
                    organizationSets.add(createOrganizationSet(tableInfo.getTableId(), indexInfos));
                }
                builder = ImmutableSet.builder();
                previousRange = nextRange;
                builder.add(sortedShards.get(previousRange));
            }
            nextRange++;
        }

        Set<ShardIndexInfo> indexInfos = builder.build();
        if (indexInfos.size() > 1) {
            organizationSets.add(createOrganizationSet(tableInfo.getTableId(), indexInfos));
        }
        return organizationSets;
    }
}
