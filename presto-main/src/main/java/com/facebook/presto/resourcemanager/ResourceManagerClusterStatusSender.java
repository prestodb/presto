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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.execution.ManagedQueryExecution;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.StatusResource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.util.PeriodicTaskExecutor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.presto.server.InternalCommunicationConfig.CommunicationProtocol.HTTP;
import static com.facebook.presto.server.InternalCommunicationConfig.CommunicationProtocol.THRIFT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ResourceManagerClusterStatusSender
        implements ClusterStatusSender
{
    private static final Logger log = Logger.get(ResourceManagerClusterStatusSender.class);

    private final DriftClient<ResourceManagerClient> thriftResourceManagerClient;
    private final HttpResourceManagerClient httpClient;
    private final InternalNodeManager internalNodeManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final Supplier<NodeStatus> statusSupplier;
    private final ScheduledExecutorService executor;
    private final Duration queryHeartbeatInterval;
    private final InternalCommunicationConfig.CommunicationProtocol communicationProtocol;

    private final Map<QueryId, PeriodicTaskExecutor> queries = new ConcurrentHashMap<>();

    private final PeriodicTaskExecutor nodeHeartbeatSender;
    private final Optional<PeriodicTaskExecutor> resourceRuntimeHeartbeatSender;

    ConcurrentMap<URI, AtomicBoolean> nodeHeartbeatInFlight = new ConcurrentHashMap<>();
    ConcurrentMap<URI, AtomicBoolean> resourceGroupHeartbeatInFlight = new ConcurrentHashMap<>();

    @Inject
    public ResourceManagerClusterStatusSender(
            @ForResourceManager DriftClient<ResourceManagerClient> thriftResourceManagerClientProvider,
            HttpResourceManagerClient httpClientProvider,
            InternalNodeManager internalNodeManager,
            StatusResource statusResource,
            @ForResourceManager ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig,
            ServerConfig serverConfig,
            InternalCommunicationConfig internalCommunicationConfig,
            ResourceGroupManager<?> resourceGroupManager)
    {
        this(
                thriftResourceManagerClientProvider,
                httpClientProvider,
                internalNodeManager,
                requireNonNull(statusResource, "statusResource is null")::getStatus,
                executor,
                resourceManagerConfig,
                serverConfig,
                internalCommunicationConfig,
                resourceGroupManager);
    }

    public ResourceManagerClusterStatusSender(
            DriftClient<ResourceManagerClient> thriftResourceManagerClient,
            HttpResourceManagerClient httpClient,
            InternalNodeManager internalNodeManager,
            Supplier<NodeStatus> statusResource,
            ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig,
            ServerConfig serverConfig,
            InternalCommunicationConfig internalCommunicationConfig,
            ResourceGroupManager<?> resourceGroupManager)
    {
        this.communicationProtocol = internalCommunicationConfig.getResourceManagerCommunicationProtocol();
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.statusSupplier = requireNonNull(statusResource, "statusResource is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.queryHeartbeatInterval = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getQueryHeartbeatInterval();
        this.nodeHeartbeatSender = new PeriodicTaskExecutor(resourceManagerConfig.getNodeHeartbeatInterval().toMillis(), executor, this::sendNodeHeartbeat);
        this.resourceRuntimeHeartbeatSender = serverConfig.isCoordinator() ? Optional.of(
                new PeriodicTaskExecutor(resourceManagerConfig.getResourceGroupRuntimeHeartbeatInterval().toMillis(), executor, this::sendResourceGroupRuntimeHeartbeat)) : Optional.empty();
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.thriftResourceManagerClient = (communicationProtocol == THRIFT) ? requireNonNull(thriftResourceManagerClient, "thriftResourceManagerClient is null") : thriftResourceManagerClient;
        this.httpClient = (communicationProtocol == HTTP) ? requireNonNull(httpClient, "httpClient is null") : httpClient;
    }

    @PostConstruct
    public void init()
    {
        nodeHeartbeatSender.start();
        if (resourceRuntimeHeartbeatSender.isPresent()) {
            resourceRuntimeHeartbeatSender.get().start();
        }
    }

    @PreDestroy
    public void stop()
    {
        queries.values().forEach(PeriodicTaskExecutor::stop);
        if (nodeHeartbeatSender != null) {
            nodeHeartbeatSender.stop();
        }
        if (resourceRuntimeHeartbeatSender.isPresent()) {
            resourceRuntimeHeartbeatSender.get().stop();
        }
    }

    @Override
    public void registerQuery(ManagedQueryExecution queryExecution)
    {
        QueryId queryId = queryExecution.getBasicQueryInfo().getQueryId();
        queries.computeIfAbsent(queryId, unused -> {
            AtomicLong sequenceId = new AtomicLong();
            ConcurrentMap<URI, AtomicBoolean> inFlight = new ConcurrentHashMap<>();

            PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(
                    queryHeartbeatInterval.toMillis(),
                    executor,
                    () -> sendQueryHeartbeat(queryExecution, sequenceId.incrementAndGet(), inFlight));
            taskExecutor.start();
            return taskExecutor;
        });
        queryExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queries.computeIfPresent(queryId, (unused, queryHeartbeatSender) -> {
                    queryHeartbeatSender.forceRun();
                    queryHeartbeatSender.stop();
                    return null;
                });
            }
        });
    }

    private void sendQueryHeartbeat(ManagedQueryExecution queryExecution, long sequenceId,
                                    ConcurrentMap<URI, AtomicBoolean> inFlightMap)
    {
        BasicQueryInfo basicQueryInfo = queryExecution.getBasicQueryInfo();
        String nodeIdentifier = internalNodeManager.getCurrentNode().getNodeIdentifier();

        if (communicationProtocol == HTTP) {
            getHttpResourceManagers().forEach(uri -> {
                AtomicBoolean inFlight = inFlightMap.computeIfAbsent(uri, ignored -> new AtomicBoolean(false));
                if (!inFlight.compareAndSet(false, true)) {
                    return;
                }
                try {
                    httpClient.queryHeartbeat(Optional.of(uri), nodeIdentifier, basicQueryInfo, sequenceId);
                }
                catch (Exception e) {
                    log.error(e, "Failed to send query heartbeat to resource manager at %s for query %s",
                            uri, basicQueryInfo.getQueryId());
                }
                finally {
                    inFlight.set(false);
                }
            });
        }
        else {
            getThriftResourceManagers().forEach(hostAndPort ->
                    thriftResourceManagerClient.get(Optional.of(hostAndPort.toString())).queryHeartbeat(nodeIdentifier, basicQueryInfo, sequenceId));
        }
    }

    private void sendNodeHeartbeat()
    {
        NodeStatus nodeStatus = statusSupplier.get();
        if (communicationProtocol == HTTP) {
            getHttpResourceManagers().forEach(uri -> {
                AtomicBoolean inFlight = nodeHeartbeatInFlight.computeIfAbsent(uri, ignored -> new AtomicBoolean(false));
                if (!inFlight.compareAndSet(false, true)) {
                    return;
                }
                try {
                    httpClient.nodeHeartbeat(Optional.of(uri), nodeStatus);
                }
                catch (Exception e) {
                    log.error(e, "Failed to send node heartbeat to resource manager at %s", uri);
                }
                finally {
                    inFlight.set(false);
                }
            });
        }
        else {
            getThriftResourceManagers().forEach(hostAndPort ->
                    thriftResourceManagerClient.get(Optional.of(hostAndPort.toString())).nodeHeartbeat(nodeStatus));
        }
    }

    public void sendResourceGroupRuntimeHeartbeat()
    {
        List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = resourceGroupManager.getResourceGroupRuntimeInfos();

        if (communicationProtocol == HTTP) {
            getHttpResourceManagers().forEach(uri -> {
                AtomicBoolean inFlight = nodeHeartbeatInFlight.computeIfAbsent(uri, ignored -> new AtomicBoolean(false));
                if (!inFlight.compareAndSet(false, true)) {
                    return;
                }
                try {
                    httpClient.resourceGroupRuntimeHeartbeat(Optional.of(uri), internalNodeManager.getCurrentNode().getNodeIdentifier(), resourceGroupRuntimeInfos);
                }
                catch (Exception e) {
                    log.error(e, "Failed to send node heartbeat to resource manager at %s", uri);
                }
                finally {
                    inFlight.set(false);
                }
            });
        }
        else {
            getThriftResourceManagers().forEach(hostAndPort ->
                    thriftResourceManagerClient.get(Optional.of(hostAndPort.toString())).resourceGroupRuntimeHeartbeat(internalNodeManager.getCurrentNode().getNodeIdentifier(), resourceGroupRuntimeInfos));
        }
    }

    private List<HostAddress> getThriftResourceManagers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(resourceManagerNode -> {
                    HostAddress hostAndPort = resourceManagerNode.getHostAndPort();
                    return HostAddress.fromParts(hostAndPort.getHostText(), resourceManagerNode.getThriftPort().getAsInt());
                })
                .collect(toImmutableList());
    }

    private List<URI> getHttpResourceManagers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .map(InternalNode::getInternalUri)
                .collect(toImmutableList());
    }
}
