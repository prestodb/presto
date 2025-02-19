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
import com.facebook.presto.router.RouterConfig;
import com.facebook.presto.router.scheduler.Scheduler;
import com.facebook.presto.router.scheduler.SchedulerFactory;
import com.facebook.presto.router.scheduler.SchedulerType;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.router.RouterUtil.parseRouterConfig;
import static com.facebook.presto.router.scheduler.SchedulerType.WEIGHTED_RANDOM_CHOICE;
import static com.facebook.presto.router.scheduler.SchedulerType.WEIGHTED_ROUND_ROBIN;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ClusterManager
{
    private Map<String, GroupSpec> groups;
    private List<SelectorRuleSpec> groupSelectors;
    private SchedulerType schedulerType;
    private Scheduler scheduler;
    private HashMap<String, HashMap<URI, Integer>> serverWeights = new HashMap<>();
    private HashMap<URI, URI> discoveryURIs = new HashMap<>();
    private final RouterConfig routerConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicLong lastConfigUpdate = new AtomicLong();
    private final RemoteInfoFactory remoteInfoFactory;
    private final Logger log = Logger.get(ClusterManager.class);

    // Cluster status
    private final ConcurrentHashMap<URI, RemoteClusterInfo> remoteClusterInfos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, RemoteQueryInfo> remoteQueryInfos = new ConcurrentHashMap<>();

    @Inject
    public ClusterManager(RouterConfig config, @ForClusterManager ScheduledExecutorService scheduledExecutorService, RemoteInfoFactory remoteInfoFactory)
    {
        this.routerConfig = config;
        this.scheduledExecutorService = scheduledExecutorService;
        RouterSpec routerSpec = parseRouterConfig(config)
                .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));
        this.groups = ImmutableMap.copyOf(routerSpec.getGroups().stream().collect(toMap(GroupSpec::getName, group -> group)));
        this.groupSelectors = ImmutableList.copyOf(routerSpec.getSelectors());
        this.schedulerType = routerSpec.getSchedulerType();
        this.scheduler = new SchedulerFactory(routerSpec.getSchedulerType()).create();
        this.remoteInfoFactory = requireNonNull(remoteInfoFactory, "remoteInfoFactory is null");
        this.initializeServerWeights();
        this.initializeMembersDiscoveryURI();
        List<URI> allClusters = getAllClusters();
        allClusters.forEach(uri -> {
            log.info("Attaching cluster %s to the router", uri.getHost());
            remoteClusterInfos.put(uri, remoteInfoFactory.createRemoteClusterInfo(discoveryURIs.get(uri)));
            remoteQueryInfos.put(uri, remoteInfoFactory.createRemoteQueryInfo(discoveryURIs.get(uri)));
            log.info("Successfully attached cluster %s to the router. Queries will be routed to cluster after successful health check", uri.getHost());
        });
    }

    @PostConstruct
    public void startConfigReloadTask()
    {
        File routerConfigFile = new File(routerConfig.getConfigFile());
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            long newConfigUpdateTime = routerConfigFile.lastModified();
            if (lastConfigUpdate.get() != newConfigUpdateTime) {
                RouterSpec routerSpec = parseRouterConfig(routerConfig)
                        .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));
                this.groups = ImmutableMap.copyOf(routerSpec.getGroups().stream().collect(toMap(GroupSpec::getName, group -> group)));
                this.groupSelectors = ImmutableList.copyOf(routerSpec.getSelectors());
                this.schedulerType = routerSpec.getSchedulerType();
                this.scheduler = new SchedulerFactory(routerSpec.getSchedulerType()).create();
                this.initializeServerWeights();
                this.initializeMembersDiscoveryURI();
                List<URI> allClusters = getAllClusters();
                allClusters.forEach(uri -> {
                    if (!remoteClusterInfos.containsKey(uri)) {
                        log.info("Attaching cluster %s to the router", uri.getHost());
                        remoteClusterInfos.put(uri, remoteInfoFactory.createRemoteClusterInfo(discoveryURIs.get(uri)));
                        remoteQueryInfos.put(uri, remoteInfoFactory.createRemoteQueryInfo(discoveryURIs.get(uri)));
                        log.info("Successfully attached cluster %s to the router. Queries will be routed to cluster after successful health check", uri.getHost());
                    }
                });
                for (URI uri : remoteClusterInfos.keySet()) {
                    if (!allClusters.contains(uri)) {
                        log.info("Removing cluster %s from the router", uri.getHost());
                        remoteClusterInfos.remove(uri);
                        remoteQueryInfos.remove(uri);
                        discoveryURIs.remove(uri);
                        log.info("Successfully removed cluster %s from the router", uri.getHost());
                    }
                }
                lastConfigUpdate.set(newConfigUpdateTime);
            }
        }, 0L, 5L, TimeUnit.SECONDS);
    }

    public void startConfigReloadTaskFileWatcher()
    {
        CompletableFuture.supplyAsync(() -> {
            try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                File routerConfigFile = new File(routerConfig.getConfigFile());
                Path parentDir = routerConfigFile.toPath().getParent();
                parentDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

                while (true) {
                    WatchKey key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        if (changed.endsWith(routerConfigFile.getName())) {
                            RouterSpec routerSpec = parseRouterConfig(routerConfig)
                                    .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));
                            this.groups = ImmutableMap.copyOf(routerSpec.getGroups().stream().collect(toMap(GroupSpec::getName, group -> group)));
                            this.groupSelectors = ImmutableList.copyOf(routerSpec.getSelectors());
                            this.schedulerType = routerSpec.getSchedulerType();
                            this.scheduler = new SchedulerFactory(routerSpec.getSchedulerType()).create();
                            this.initializeServerWeights();
                            this.initializeMembersDiscoveryURI();
                            List<URI> allClusters = getAllClusters();

                            allClusters.forEach(uri -> {
                                if (!remoteClusterInfos.containsKey(uri)) {
                                    log.info("Attaching cluster %s to the router", uri.getHost());
                                    remoteClusterInfos.put(uri, remoteInfoFactory.createRemoteClusterInfo(discoveryURIs.get(uri)));
                                    remoteQueryInfos.put(uri, remoteInfoFactory.createRemoteQueryInfo(discoveryURIs.get(uri)));
                                    log.info("Successfully attached cluster %s to the router. Queries will be routed to cluster after successful health check", uri.getHost());
                                }
                            });

                            for (URI uri : remoteClusterInfos.keySet()) {
                                if (!allClusters.contains(uri)) {
                                    log.info("Removing cluster %s from the router", uri.getHost());
                                    remoteClusterInfos.remove(uri);
                                    remoteQueryInfos.remove(uri);
                                    discoveryURIs.remove(uri);
                                    log.info("Successfully removed cluster %s from the router", uri.getHost());
                                }
                            }
                        }
                        key.reset();
                    }
                }
            }
            catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public List<URI> getAllClusters()
    {
        return groups.values().stream()
                .flatMap(groupSpec -> groupSpec.getMembers().stream())
                .collect(toImmutableList());
    }

    public Optional<URI> getDestination(RequestInfo requestInfo)
    {
        Optional<String> target = matchGroup(requestInfo);
        if (!target.isPresent()) {
            return Optional.empty();
        }

        checkArgument(groups.containsKey(target.get()));
        GroupSpec groupSpec = groups.get(target.get());

        List<URI> healthyClusterURIs = groupSpec.getMembers().stream()
                .filter(entry -> remoteClusterInfos.get(entry).isHealthy())
                .collect(Collectors.toList());

        if (healthyClusterURIs.isEmpty()) {
            log.info("Healthy cluster not found!");
            return Optional.empty();
        }

        scheduler.setCandidates(healthyClusterURIs);
        if (schedulerType == WEIGHTED_RANDOM_CHOICE || schedulerType == WEIGHTED_ROUND_ROBIN) {
            scheduler.setWeights(serverWeights.get(groupSpec.getName()));
        }
        return scheduler.getDestination(requestInfo.getUser());
    }

    private Optional<String> matchGroup(RequestInfo requestInfo)
    {
        return groupSelectors.stream()
                .map(s -> s.match(requestInfo))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private void initializeServerWeights()
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

    private void initializeMembersDiscoveryURI()
    {
        groups.forEach((name, groupSpec) -> {
            List<URI> members = groupSpec.getMembers();
            List<URI> membersDiscoveryURI = groupSpec.getMembersDiscoveryURI();
            for (int i = 0; i < members.size(); i++) {
                discoveryURIs.put(members.get(i), membersDiscoveryURI.get(i));
            }
        });
    }

    public ConcurrentHashMap<URI, RemoteClusterInfo> getRemoteClusterInfos()
    {
        return remoteClusterInfos;
    }

    public ConcurrentHashMap<URI, RemoteQueryInfo> getRemoteQueryInfos()
    {
        return remoteQueryInfos;
    }
}
