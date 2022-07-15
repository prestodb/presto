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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ClusterStatusTracker
{
    private static final Logger log = Logger.get(ClusterStatusTracker.class);

    private final ClusterManager clusterManager;
    private final RemoteInfoFactory remoteInfoFactory;
    private final ScheduledExecutorService queryInfoUpdateExecutor;

    // Cluster status
    private final ConcurrentHashMap<URI, RemoteClusterInfo> remoteClusterInfos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, RemoteQueryInfo> remoteQueryInfos = new ConcurrentHashMap<>();

    @Inject
    public ClusterStatusTracker(
            ClusterManager clusterManager,
            RemoteInfoFactory remoteInfoFactory)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
        this.remoteInfoFactory = requireNonNull(remoteInfoFactory, "remoteInfoFactory is null");
        this.queryInfoUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("query-info-poller-%s"));
    }

    @PostConstruct
    public void startPollingQueryInfo()
    {
        clusterManager.getAllClusters().forEach(uri -> {
            remoteClusterInfos.put(uri, remoteInfoFactory.createRemoteClusterInfo(uri));
            remoteQueryInfos.put(uri, remoteInfoFactory.createRemoteQueryInfo(uri));
        });

        queryInfoUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollQueryInfos();
            }
            catch (Exception e) {
                log.error(e, "Error polling list of queries");
            }
        }, 5, 5, TimeUnit.SECONDS);

        pollQueryInfos();
    }

    private void pollQueryInfos()
    {
        ImmutableSet<URI> allClusters = ImmutableSet.copyOf(clusterManager.getAllClusters());
        ImmutableSet<URI> inactiveClusters = difference(remoteQueryInfos.keySet(), allClusters).immutableCopy();
        remoteQueryInfos.keySet().removeAll(inactiveClusters);

        allClusters.forEach(uri -> {
            remoteClusterInfos.putIfAbsent(uri, remoteInfoFactory.createRemoteClusterInfo(uri));
            remoteQueryInfos.putIfAbsent(uri, remoteInfoFactory.createRemoteQueryInfo(uri));
        });

        remoteClusterInfos.values().forEach(RemoteClusterInfo::asyncRefresh);
        remoteQueryInfos.values().forEach(RemoteQueryInfo::asyncRefresh);
    }

    public long getRunningQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getRunningQueries)
                .sum();
    }

    public long getBlockedQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getBlockedQueries)
                .sum();
    }

    public long getQueuedQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getQueuedQueries)
                .sum();
    }

    public long getClusterCount()
    {
        return remoteClusterInfos.entrySet().size();
    }

    public long getActiveWorkers()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getActiveWorkers)
                .sum();
    }

    public long getRunningDrivers()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getRunningDrivers)
                .sum();
    }

    public List<JsonNode> getAllQueryInfos()
    {
        ImmutableList.Builder<JsonNode> builder = ImmutableList.builder();
        remoteQueryInfos.forEach((coordinator, remoteQueryInfo) ->
                builder.addAll(remoteQueryInfo.getQueryList().orElse(ImmutableList.of()).stream()
                        .map(queryInfo -> ((ObjectNode) queryInfo).put("coordinatorUri", coordinator.toASCIIString()))
                        .collect(toImmutableList())));
        return builder.build();
    }
}
