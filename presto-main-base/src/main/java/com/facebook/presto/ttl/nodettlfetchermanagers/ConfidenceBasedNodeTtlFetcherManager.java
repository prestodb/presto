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
package com.facebook.presto.ttl.nodettlfetchermanagers;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcher;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;
import com.facebook.presto.util.PeriodicTaskExecutor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.difference;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ConfidenceBasedNodeTtlFetcherManager
        implements NodeTtlFetcherManager
{
    private static final Logger log = Logger.get(ConfidenceBasedNodeTtlFetcherManager.class);
    private static final File TTL_FETCHER_CONFIG = new File("etc/node-ttl-fetcher.properties");
    private static final String TTL_FETCHER_PROPERTY_NAME = "node-ttl-fetcher.factory";
    private final AtomicReference<NodeTtlFetcher> ttlFetcher = new AtomicReference<>();
    private final InternalNodeManager nodeManager;
    private final ConcurrentHashMap<InternalNode, NodeTtl> nodeTtlMap = new ConcurrentHashMap<>();
    private final boolean isWorkScheduledOnCoordinator;
    private final Map<String, NodeTtlFetcherFactory> ttlFetcherFactories = new ConcurrentHashMap<>();
    private final NodeTtlFetcherManagerConfig nodeTtlFetcherManagerConfig;
    private final AtomicLong lastRefreshEpochMillis = new AtomicLong(Long.MAX_VALUE);
    private final CounterStat refreshFailures = new CounterStat();
    private final Consumer<AllNodes> nodeChangeListener = this::refreshTtlInfo;
    private PeriodicTaskExecutor periodicTtlRefresher;

    @Inject
    public ConfidenceBasedNodeTtlFetcherManager(InternalNodeManager nodeManager, NodeSchedulerConfig schedulerConfig, NodeTtlFetcherManagerConfig nodeTtlFetcherManagerConfig)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.isWorkScheduledOnCoordinator = requireNonNull(schedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
        this.nodeTtlFetcherManagerConfig = requireNonNull(nodeTtlFetcherManagerConfig, "nodeTtlFetcherManagerConfig is null");
    }

    private static long jitterForPeriodicRefresh(long delayMillis)
    {
        double maxJitter = delayMillis * 0.1;
        return round(delayMillis + (maxJitter * (2 * ThreadLocalRandom.current().nextDouble() - 1)));
    }

    public void scheduleRefresh()
    {
        periodicTtlRefresher = new PeriodicTaskExecutor(
                ttlFetcher.get().getRefreshInterval().toMillis(),
                nodeTtlFetcherManagerConfig.getInitialDelayBeforeRefresh().toMillis(),
                newSingleThreadScheduledExecutor(threadsNamed("refresh-node-ttl-executor-%s")),
                this::refreshTtlInfo,
                ConfidenceBasedNodeTtlFetcherManager::jitterForPeriodicRefresh);
        periodicTtlRefresher.start();
    }

    @PreDestroy
    public void stop()
    {
        nodeManager.removeNodeChangeListener(nodeChangeListener);
        if (periodicTtlRefresher != null) {
            periodicTtlRefresher.stop();
        }
    }

    @VisibleForTesting
    public synchronized void refreshTtlInfo()
    {
        AllNodes allNodes = nodeManager.getAllNodes();
        refreshTtlInfo(allNodes);
    }

    private synchronized void refreshTtlInfo(AllNodes allNodes)
    {
        try {
            Set<InternalNode> activeWorkers = Sets.difference(allNodes.getActiveNodes(), allNodes.getActiveResourceManagers());

            if (!isWorkScheduledOnCoordinator) {
                activeWorkers = Sets.difference(activeWorkers, allNodes.getActiveCoordinators());
            }

            Map<NodeInfo, InternalNode> internalNodeMap = activeWorkers
                    .stream()
                    .collect(toImmutableMap(node -> new NodeInfo(node.getNodeIdentifier(), node.getHost()), Function.identity()));

            Map<NodeInfo, NodeTtl> ttlInfo = ttlFetcher.get().getTtlInfo(ImmutableSet.copyOf(internalNodeMap.keySet()));

            nodeTtlMap.putAll(ttlInfo.entrySet().stream().collect(toImmutableMap(e -> internalNodeMap.get(e.getKey()), Map.Entry::getValue)));

            Set<InternalNode> deadNodes = difference(nodeTtlMap.keySet(), activeWorkers).immutableCopy();
            nodeTtlMap.keySet().removeAll(deadNodes);

            log.info("Node ttls refreshed, nodeTtlMap: %s", nodeTtlMap);

            lastRefreshEpochMillis.set(currentTimeMillis());
        }
        catch (Throwable e) {
            refreshFailures.update(1);
            log.error(e, "Error loading node ttls");
        }
    }

    public Optional<NodeTtl> getTtlInfo(InternalNode node)
    {
        return nodeTtlMap.containsKey(node) ? Optional.of(nodeTtlMap.get(node)) : Optional.empty();
    }

    @Override
    public Map<InternalNode, NodeTtl> getAllTtls()
    {
        return ImmutableMap.copyOf(nodeTtlMap);
    }

    @Override
    public void addNodeTtlFetcherFactory(NodeTtlFetcherFactory nodeTtlFetcherFactory)
    {
        requireNonNull(nodeTtlFetcherFactory, "nodeTtlFetcherFactory is null");

        if (ttlFetcherFactories.putIfAbsent(nodeTtlFetcherFactory.getName(), nodeTtlFetcherFactory) != null) {
            throw new IllegalArgumentException(format("Node ttl fetcher factory '%s' is already registered", nodeTtlFetcherFactory.getName()));
        }
    }

    @Override
    public void loadNodeTtlFetcher()
            throws Exception
    {
        String factoryName = "infinite";
        Map<String, String> properties = ImmutableMap.of();

        if (TTL_FETCHER_CONFIG.exists()) {
            properties = new HashMap<>(loadProperties(TTL_FETCHER_CONFIG));
            factoryName = properties.remove(TTL_FETCHER_PROPERTY_NAME);

            checkArgument(!isNullOrEmpty(factoryName),
                    "Node ttl fetcher configuration %s does not contain %s", TTL_FETCHER_CONFIG.getAbsoluteFile(), TTL_FETCHER_PROPERTY_NAME);
        }

        load(factoryName, properties);

        if (ttlFetcher.get().needsPeriodicRefresh()) {
            scheduleRefresh();
        }
        else {
            refreshTtlInfo();
        }

        nodeManager.addNodeChangeListener(nodeChangeListener);
    }

    @VisibleForTesting
    public void load(String factoryName, Map<String, String> properties)
    {
        log.info("-- Loading node ttl fetcher factory --");

        NodeTtlFetcherFactory nodeTtlFetcherFactory = ttlFetcherFactories.get(factoryName);
        checkState(nodeTtlFetcherFactory != null, "Node ttl fetcher factory %s is not registered", factoryName);

        NodeTtlFetcher nodeTtlFetcher = nodeTtlFetcherFactory.create(properties);
        checkState(this.ttlFetcher.compareAndSet(null, nodeTtlFetcher), "Node ttl fetcher has already been set!");

        log.info("-- Loaded node ttl fetcher %s --", factoryName);
    }

    @Managed
    public long getTimeInMillisSinceLastTtlRefresh()
    {
        if (lastRefreshEpochMillis.get() == Long.MAX_VALUE) {
            return -1;
        }
        return currentTimeMillis() - lastRefreshEpochMillis.get();
    }

    @Managed
    public long getStaleTtlWorkerCount()
    {
        Duration staleDuration = nodeTtlFetcherManagerConfig.getStaleTtlThreshold();
        Instant staleInstant = Instant.now().minus(staleDuration.toMillis(), ChronoUnit.MILLIS);
        return nodeTtlMap.values().stream().filter(nodeTtl -> nodeTtl.getTtlPredictionInstant().isBefore(staleInstant)).count();
    }

    @Managed
    @Nested
    public CounterStat getRefreshFailures()
    {
        return refreshFailures;
    }
}
