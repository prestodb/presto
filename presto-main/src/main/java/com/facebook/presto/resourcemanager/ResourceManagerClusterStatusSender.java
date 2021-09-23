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

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.execution.ManagedQueryExecution;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.server.StatusResource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.util.PeriodicTaskExecutor;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ResourceManagerClusterStatusSender
        implements ClusterStatusSender
{
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final InternalNodeManager internalNodeManager;
    private final Supplier<NodeStatus> statusSupplier;
    private final ScheduledExecutorService executor;
    private final Duration queryHeartbeatInterval;

    private final Map<QueryId, PeriodicTaskExecutor> queries = new ConcurrentHashMap<>();

    private final PeriodicTaskExecutor nodeHeartbeatSender;

    @Inject
    public ResourceManagerClusterStatusSender(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            InternalNodeManager internalNodeManager,
            StatusResource statusResource,
            @ForResourceManager ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        requireNonNull(statusResource, "statusResource is null");
        this.statusSupplier = statusResource::getStatus;
        this.executor = requireNonNull(executor, "executor is null");
        this.queryHeartbeatInterval = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getQueryHeartbeatInterval();
        nodeHeartbeatSender = new PeriodicTaskExecutor(resourceManagerConfig.getNodeHeartbeatInterval().toMillis(), executor, this::sendNodeHeartbeat);
    }

    public ResourceManagerClusterStatusSender(
            DriftClient<ResourceManagerClient> resourceManagerClient,
            InternalNodeManager internalNodeManager,
            Supplier<NodeStatus> statusResource,
            ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.statusSupplier = requireNonNull(statusResource, "statusResource is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.queryHeartbeatInterval = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getQueryHeartbeatInterval();
        nodeHeartbeatSender = new PeriodicTaskExecutor(resourceManagerConfig.getNodeHeartbeatInterval().toMillis(), executor, this::sendNodeHeartbeat);
    }

    @PostConstruct
    public void init()
    {
        nodeHeartbeatSender.start();
    }

    @PreDestroy
    public void stop()
    {
        queries.values().forEach(PeriodicTaskExecutor::stop);
        if (nodeHeartbeatSender != null) {
            nodeHeartbeatSender.stop();
        }
    }

    @Override
    public void registerQuery(ManagedQueryExecution queryExecution)
    {
        QueryId queryId = queryExecution.getBasicQueryInfo().getQueryId();
        queries.computeIfAbsent(queryId, unused -> {
            AtomicLong sequenceId = new AtomicLong();
            PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(
                    queryHeartbeatInterval.toMillis(),
                    executor,
                    () -> sendQueryHeartbeat(queryExecution, sequenceId.incrementAndGet()));
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

    private void sendQueryHeartbeat(ManagedQueryExecution queryExecution, long sequenceId)
    {
        BasicQueryInfo basicQueryInfo = queryExecution.getBasicQueryInfo();
        String nodeIdentifier = internalNodeManager.getCurrentNode().getNodeIdentifier();
        getResourceManagers().forEach(hostAndPort ->
                resourceManagerClient.get(Optional.of(hostAndPort.toString())).queryHeartbeat(nodeIdentifier, basicQueryInfo, sequenceId));
    }

    private void sendNodeHeartbeat()
    {
        getResourceManagers().forEach(hostAndPort ->
                resourceManagerClient.get(Optional.of(hostAndPort.toString())).nodeHeartbeat(statusSupplier.get()));
    }

    private List<HostAddress> getResourceManagers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(resourceManagerNode -> {
                    HostAddress hostAndPort = resourceManagerNode.getHostAndPort();
                    return HostAddress.fromParts(hostAndPort.getHostText(), resourceManagerNode.getThriftPort().getAsInt());
                })
                .collect(toImmutableList());
    }
}
