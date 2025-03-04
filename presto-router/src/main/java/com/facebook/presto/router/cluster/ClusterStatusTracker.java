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
import com.google.inject.Inject;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ClusterStatusTracker
{
    private static final Logger log = Logger.get(ClusterStatusTracker.class);

    private final ClusterManager clusterManager;
    private final ScheduledExecutorService queryInfoUpdateExecutor;

    @Inject
    public ClusterStatusTracker(
            ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
        this.queryInfoUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("query-info-poller-%s"));
    }

    @PostConstruct
    public void startPollingQueryInfo()
    {
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
        clusterManager.getRemoteClusterInfos().values().forEach(RemoteClusterInfo::asyncRefresh);
        clusterManager.getRemoteQueryInfos().values().forEach(RemoteQueryInfo::asyncRefresh);
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
        return clusterManager.getRemoteClusterInfos().entrySet().size();
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
