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
package com.facebook.presto.router;

import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.facebook.presto.router.cluster.RemoteStateConfig;
import com.facebook.presto.router.scheduler.SchedulerFactory;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.router.RouterUtil.parseRouterConfig;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.stream.Collectors.toMap;

public class BarrierClusterManager
        extends ClusterManager
{
    private final CyclicBarrier barrier;

    public BarrierClusterManager(RouterConfig config, RemoteInfoFactory remoteInfoFactory, RemoteStateConfig remoteStateConfig, CyclicBarrier barrier)
    {
        super(config, remoteInfoFactory, remoteStateConfig);
        this.barrier = barrier;

        this.onConfigChangeDetection = () -> {
            try {
                RouterSpec updateRouterSpec = parseRouterConfig(routerConfig)
                        .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));
                this.groups = ImmutableMap.copyOf(updateRouterSpec.getGroups().stream().collect(toMap(GroupSpec::getName, group -> group)));
                this.groupSelectors = ImmutableList.copyOf(updateRouterSpec.getSelectors());
                this.scheduler = new SchedulerFactory(updateRouterSpec.getSchedulerType()).create();
                this.initializeServerWeights();
                this.initializeMembersDiscoveryURI();
                List<URI> updatedAllClusters = getAllClusters();

                updatedAllClusters.forEach(uri -> {
                    if (!getRemoteClusterInfos().containsKey(uri)) {
                        log.info("Attaching cluster %s to the router", uri.getHost());
                        getRemoteClusterInfos().put(uri, remoteInfoFactory.createRemoteClusterInfo(discoveryURIs.get(uri)));
                        getRemoteQueryInfos().put(uri, remoteInfoFactory.createRemoteQueryInfo(discoveryURIs.get(uri)));
                        log.info("Successfully attached cluster %s to the router. Queries will be routed to cluster after successful health check", uri.getHost());
                    }
                });

                for (URI uri : getRemoteClusterInfos().keySet()) {
                    if (!updatedAllClusters.contains(uri)) {
                        log.info("Removing cluster %s from the router", uri.getHost());
                        getRemoteClusterInfos().remove(uri);
                        getRemoteQueryInfos().remove(uri);
                        discoveryURIs.remove(uri);
                        log.info("Successfully removed cluster %s from the router", uri.getHost());
                    }
                }
                this.barrier.await(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException("Barrier synchronization failed", e);
            }
        };
        startConfigReloadTaskFileWatcher();
    }
}
