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
package com.facebook.presto.ttl;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ConfidenceBasedTTLFetcherManager
        implements TTLFetcherManager
{
    private static final Logger log = Logger.get(ConfidenceBasedTTLFetcherManager.class);
    private final TTLFetcher ttlFetcher;
    private final ScheduledExecutorService refreshTtlExecutor;
    private final InternalNodeManager nodeManager;
    private final ConcurrentHashMap<InternalNode, NodeTTL> nodeTtlMap = new ConcurrentHashMap<>();
    private final boolean isWorkScheduledOnCoordinator;

    @Inject
    public ConfidenceBasedTTLFetcherManager(TTLFetcher ttlFetcher, InternalNodeManager nodeManager, NodeSchedulerConfig schedulerConfig)
    {
        this.ttlFetcher = ttlFetcher;
        this.nodeManager = nodeManager;
        this.isWorkScheduledOnCoordinator = schedulerConfig.isIncludeCoordinator();
        refreshTtlExecutor = newSingleThreadScheduledExecutor(threadsNamed("refresh-ttl-executor-%s"));
    }

    @PostConstruct
    public void start()
    {
        refreshTtlExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshTtlInfo();
            }
            catch (Exception e) {
                log.error(e, "Error fetching TTLs!");
            }
//         TODO: Make configurable
        }, 60, 1000, TimeUnit.SECONDS);
        refreshTtlInfo();
    }

    @PreDestroy
    public void stop()
    {
        refreshTtlExecutor.shutdownNow();
    }

    @VisibleForTesting
    private synchronized void refreshTtlInfo()
    {
        AllNodes allNodes = nodeManager.getAllNodes();
        Set<InternalNode> activeWorkers = Sets.difference(allNodes.getActiveNodes(), allNodes.getActiveResourceManagers());

        if (!isWorkScheduledOnCoordinator) {
            activeWorkers = Sets.difference(activeWorkers, allNodes.getActiveCoordinators());
        }

        log.info("TTL FETCHERS: %s", ttlFetcher);
        Map<InternalNode, NodeTTL> ttlInfo = ttlFetcher.getTTLInfo(activeWorkers);

        nodeTtlMap.putAll(ttlInfo);

        log.info("Pre-filter nodeTtlMap: %s", nodeTtlMap);

        Set<InternalNode> deadNodes = difference(nodeTtlMap.keySet(), activeWorkers).immutableCopy();
        nodeTtlMap.keySet().removeAll(deadNodes);

        log.info("TTLs refreshed, nodeTtlMap: %s", nodeTtlMap);
    }

    public Optional<NodeTTL> getTTLInfo(InternalNode node)
    {
        return nodeTtlMap.containsKey(node) ? Optional.of(nodeTtlMap.get(node)) : Optional.empty();
    }

    @Override
    public Map<InternalNode, NodeTTL> getAllTTLs()
    {
        return ImmutableMap.copyOf(nodeTtlMap);
    }
}
