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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.router.RouterConfig;
import com.facebook.presto.router.scheduler.CustomSchedulerManager;
import com.facebook.presto.router.scheduler.SchedulerFactory;
import com.facebook.presto.router.scheduler.SchedulerType;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.router.Scheduler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sun.nio.file.SensitivityWatchEventModifier;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.net.URI;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.router.RouterUtil.parseRouterConfig;
import static com.facebook.presto.router.scheduler.SchedulerType.CUSTOM_PLUGIN_SCHEDULER;
import static com.facebook.presto.router.scheduler.SchedulerType.ROUND_ROBIN;
import static com.facebook.presto.router.scheduler.SchedulerType.WEIGHTED_RANDOM_CHOICE;
import static com.facebook.presto.router.scheduler.SchedulerType.WEIGHTED_ROUND_ROBIN;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClusterManager
        implements AutoCloseable
{
    private final AtomicReference<ClusterManagerConfig> currentConfig = new AtomicReference<>();

    private final Path configFile;
    private final Logger log = Logger.get(ClusterManager.class);

    // Cluster status
    private final Map<URI, RemoteClusterInfo> remoteClusterInfos = new ConcurrentHashMap<>();
    private final Map<URI, RemoteQueryInfo> remoteQueryInfos = new ConcurrentHashMap<>();

    private final AtomicBoolean isWatchServiceStarted = new AtomicBoolean();
    private final RemoteInfoFactory remoteInfoFactory;
    private final HashMap<String, HashMap<URI, Integer>> serverWeights = new HashMap<>();
    private final CustomSchedulerManager schedulerManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledFuture<?> configDetection;
    private final WatchService watchService;
    private final WatchKey watchKey;

    @Inject
    public ClusterManager(RouterConfig config, RemoteInfoFactory remoteInfoFactory, CustomSchedulerManager schedulerManager)
            throws IOException
    {
        this.configFile = Paths.get(requireNonNull(config, "config is null").getConfigFile());
        this.remoteInfoFactory = requireNonNull(remoteInfoFactory, "remoteInfoFactory is null");
        this.schedulerManager = schedulerManager;
        reloadConfig();
        initializeServerWeights();
        watchService = FileSystems.getDefault().newWatchService();
        Path parentDir = configFile.getParent();
        log.info("Router config watch service monitoring %s", parentDir);
        watchKey = parentDir.register(watchService,
                new WatchEvent.Kind[] {ENTRY_CREATE, ENTRY_MODIFY},
                SensitivityWatchEventModifier.HIGH);
        log.info("Successfully registered watch service for %s", parentDir);
        scheduledExecutorService = newSingleThreadScheduledExecutor();
        configDetection = scheduledExecutorService.scheduleAtFixedRate(this::monitorConfig, 0, 3, SECONDS);
    }

    protected void monitorConfig()
    {
        boolean reload = false;
        try {
            WatchKey key = watchService.poll(1, SECONDS);
            if (key == null) {
                return;
            }
            List<WatchEvent<?>> events = key.pollEvents();
            log.debug("Changes to router config directory detected");
            for (WatchEvent<?> event : events) {
                log.debug("Event detected: %s, path: %s", event.kind().name(), event.context());
                Path changed = (Path) event.context();
                if (changed.endsWith(configFile.getFileName())) {
                    reload = true;
                    break;
                }
                else {
                    log.debug("Change to %s ignored by ClusterManager (config file is %s)", event.context(), configFile);
                }
            }
            key.reset();
        }
        catch (ClosedWatchServiceException e) {
            log.warn("Watch service closed. Future updates configuration changes will not be detected.");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Watch service interrupted while waiting for configuration updates");
        }

        if (reload) {
            reloadConfig();
        }
    }

    protected void reloadConfig()
    {
        RouterSpec newRouterSpec = parseRouterConfig(configFile)
                .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));
        Map<String, GroupSpec> newGroups = newRouterSpec.getGroups().stream().collect(toImmutableMap(GroupSpec::getName, group -> group));
        List<SelectorRuleSpec> newGroupSelectors = ImmutableList.copyOf(newRouterSpec.getSelectors());
        Scheduler newScheduler = new SchedulerFactory(newRouterSpec.getSchedulerType(), schedulerManager).create();
        SchedulerType newSchedulerType = newRouterSpec.getSchedulerType();

        List<URI> updatedAllClusters = newGroups.values().stream()
                .flatMap(groupSpec -> groupSpec.getMembers().stream())
                .collect(toImmutableList());

        Map<URI, URI> newDiscoveryURIs = new HashMap<>();
        initializeMembersDiscoveryURI(newDiscoveryURIs, newGroups);

        updatedAllClusters.forEach(uri -> {
            remoteClusterInfos.computeIfAbsent(uri, value -> remoteInfoFactory.createRemoteClusterInfo(newDiscoveryURIs.get(value)));
            remoteQueryInfos.computeIfAbsent(uri, value -> remoteInfoFactory.createRemoteQueryInfo(newDiscoveryURIs.get(value)));
            log.debug("Attached cluster %s to the router. Queries will be routed to cluster after successful health check", uri.getHost());
        });

        for (URI uri : remoteClusterInfos.keySet()) {
            if (!updatedAllClusters.contains(uri)) {
                remoteClusterInfos.remove(uri);
                remoteQueryInfos.remove(uri);
                log.info("Removed cluster %s from the router", uri.getHost());
            }
        }
        currentConfig.set(new ClusterManagerConfig(newGroups, newGroupSelectors, newScheduler, newSchedulerType));
    }

    public List<URI> getAllClusters()
    {
        return currentConfig.get().getGroups().values().stream()
                .flatMap(groupSpec -> groupSpec.getMembers().stream())
                .collect(toImmutableList());
    }

    public Optional<URI> getDestination(RequestInfo requestInfo)
    {
        ClusterManagerConfig config = currentConfig.get();
        Optional<String> target = matchGroup(requestInfo);
        if (!target.isPresent()) {
            return Optional.empty();
        }

        checkArgument(config.getGroups().containsKey(target.get()));
        GroupSpec groupSpec = config.getGroups().get(target.get());

        List<URI> healthyClusterURIs = groupSpec.getMembers().stream().filter((entry) ->
                        Optional.ofNullable(remoteClusterInfos.get(entry))
                                .map(RemoteClusterInfo::isHealthy)
                                .orElse(false))
                .collect(Collectors.toList());

        if (healthyClusterURIs.isEmpty()) {
            log.debug("No healthy cluster found, will attempt to route using existing group spec");
            healthyClusterURIs = groupSpec.getMembers();
        }
        log.debug("Available clusters: %s", healthyClusterURIs);

        config.getScheduler().setCandidates(healthyClusterURIs);
        if (config.getSchedulerType() == WEIGHTED_RANDOM_CHOICE || config.getSchedulerType() == WEIGHTED_ROUND_ROBIN) {
            config.getScheduler().setWeights(config.getServerWeights().get(groupSpec.getName()));
        }
        else if (config.getSchedulerType() == CUSTOM_PLUGIN_SCHEDULER) {
            try {
                //Set remote cluster infos in the custom plugin scheduler
                Map<URI, RemoteClusterInfo> healthyRemoteClusterInfos = Maps.filterValues(remoteClusterInfos, RemoteState::isHealthy);
                config.getScheduler().setClusterInfos(ImmutableMap.copyOf(healthyRemoteClusterInfos));

                return config.getScheduler().getDestination(requestInfo.toRouterRequestInfo());
            }
            catch (Exception e) {
                log.error("Custom Plugin Scheduler failed to schedule the query! Failed because of : %s", e);
                return Optional.empty();
            }
        }

        if (config.getSchedulerType() == ROUND_ROBIN || config.getSchedulerType() == WEIGHTED_ROUND_ROBIN) {
            config.getScheduler().setCandidateGroupName(target.get());
        }

        return config.getScheduler().getDestination(requestInfo.toRouterRequestInfo());
    }

    private Optional<String> matchGroup(RequestInfo requestInfo)
    {
        return currentConfig.get().groupSelectors.stream()
                .map(s -> s.match(requestInfo))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private void initializeServerWeights()
    {
        currentConfig.get().getGroups().forEach((name, groupSpec) -> {
            List<URI> members = groupSpec.getMembers();
            List<Integer> weights = groupSpec.getWeights();
            serverWeights.put(name, new HashMap<>());
            for (int i = 0; i < members.size(); i++) {
                serverWeights.get(name).put(members.get(i), weights.get(i));
            }
        });
    }

    private void initializeMembersDiscoveryURI(Map<URI, URI> discoveryURIs, Map<String, GroupSpec> groups)
    {
        groups.forEach((name, groupSpec) -> {
            List<URI> members = groupSpec.getMembers();
            List<URI> membersDiscoveryURI = groupSpec.getMembersDiscoveryURI();
            for (int i = 0; i < members.size(); i++) {
                discoveryURIs.put(members.get(i), membersDiscoveryURI.get(i));
            }
        });
    }

    @VisibleForTesting
    public Map<URI, RemoteClusterInfo> getRemoteClusterInfos()
    {
        return remoteClusterInfos;
    }

    @VisibleForTesting
    public Map<URI, RemoteQueryInfo> getRemoteQueryInfos()
    {
        return remoteQueryInfos;
    }

    @VisibleForTesting
    public boolean getIsWatchServiceStarted()
    {
        return isWatchServiceStarted.get();
    }

    @PreDestroy
    @Override
    public void close()
            throws Exception
    {
        try {
            watchKey.cancel();
            watchService.close();
        }
        finally {
            configDetection.cancel(true);
            scheduledExecutorService.shutdownNow();
        }
    }

    public static class ClusterStatusTracker
    {
        private final Logger log = Logger.get(ClusterStatusTracker.class);

        private final ClusterManager clusterManager;
        private final ScheduledExecutorService queryInfoUpdateExecutor;
        private final Duration pollingInterval;

        @Inject
        public ClusterStatusTracker(ClusterManager clusterManager, RemoteStateConfig remoteStateConfig)
        {
            this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
            this.queryInfoUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("query-info-poller-%s"));
            this.pollingInterval = remoteStateConfig.getPollingInterval();

            this.clusterManager.getRemoteClusterInfos().values().forEach(RemoteClusterInfo::asyncRefresh);
            this.clusterManager.getRemoteQueryInfos().values().forEach(RemoteQueryInfo::asyncRefresh);

            startPollingQueryInfo();
        }

        public void startPollingQueryInfo()
        {
            queryInfoUpdateExecutor.scheduleWithFixedDelay(() -> {
                try {
                    clusterManager.getRemoteClusterInfos().values().forEach(RemoteClusterInfo::asyncRefresh);
                    clusterManager.getRemoteQueryInfos().values().forEach(RemoteQueryInfo::asyncRefresh);
                }
                catch (Exception e) {
                    log.error(e, "Error polling list of queries");
                }
            }, pollingInterval.toMillis(), pollingInterval.toMillis(), MILLISECONDS);
        }

        @Managed
        public long getRunningQueries()
        {
            return clusterManager.getRemoteClusterInfos().values().stream()
                    .mapToLong(RemoteClusterInfo::getRunningQueries)
                    .sum();
        }

        @Managed
        public long getBlockedQueries()
        {
            return clusterManager.getRemoteClusterInfos().values().stream()
                    .mapToLong(RemoteClusterInfo::getBlockedQueries)
                    .sum();
        }

        @Managed
        public long getQueuedQueries()
        {
            return clusterManager.getRemoteClusterInfos().values().stream()
                    .mapToLong(RemoteClusterInfo::getQueuedQueries)
                    .sum();
        }

        @Managed
        public long getClusterCount()
        {
            return clusterManager.getRemoteClusterInfos().size();
        }

        @Managed
        public long getActiveWorkers()
        {
            return clusterManager.getRemoteClusterInfos().values().stream()
                    .mapToLong(RemoteClusterInfo::getActiveWorkers)
                    .sum();
        }

        @Managed
        public long getRunningDrivers()
        {
            return clusterManager.getRemoteClusterInfos().values().stream()
                    .mapToLong(RemoteClusterInfo::getRunningDrivers)
                    .sum();
        }

        public List<JsonNode> getAllQueryInfos()
        {
            ImmutableList.Builder<JsonNode> builder = ImmutableList.builder();
            clusterManager.getRemoteQueryInfos().forEach((coordinator, remoteQueryInfo) ->
                    builder.addAll(remoteQueryInfo.getQueryList().orElse(ImmutableList.of()).stream()
                            .map(queryInfo -> ((ObjectNode) queryInfo).put("coordinatorUri", coordinator.toASCIIString()))
                            .collect(toImmutableList())));
            return builder.build();
        }
    }

    private static class ClusterManagerConfig
    {
        private final Map<String, GroupSpec> groups;
        private final List<SelectorRuleSpec> groupSelectors;
        private final Scheduler scheduler;
        private final SchedulerType schedulerType;
        private final Map<String, Map<URI, Integer>> serverWeights = new HashMap<>();

        public ClusterManagerConfig(
                Map<String, GroupSpec> groups,
                List<SelectorRuleSpec> groupSelectors,
                Scheduler scheduler,
                SchedulerType schedulerType)
        {
            this.groups = groups;
            this.groupSelectors = groupSelectors;
            this.scheduler = scheduler;
            this.schedulerType = schedulerType;
            initializeServerWeights();
        }

        protected void initializeServerWeights()
        {
            groups.forEach((name, groupSpec) -> {
                List<URI> members = groupSpec.getMembers();
                List<Integer> weights = groupSpec.getWeights();
                serverWeights.put(name, new HashMap<>());
                for (int i = 0; i < members.size(); i++) {
                    serverWeights.get(name).put(members.get(i), weights.get(i));
                }
            });
        }

        public Map<String, GroupSpec> getGroups()
        {
            return groups;
        }

        public Scheduler getScheduler()
        {
            return scheduler;
        }

        public SchedulerType getSchedulerType()
        {
            return schedulerType;
        }

        public Map<String, Map<URI, Integer>> getServerWeights()
        {
            return serverWeights;
        }
    }
}
