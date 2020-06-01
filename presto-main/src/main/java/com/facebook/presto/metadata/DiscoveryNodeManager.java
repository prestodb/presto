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
package com.facebook.presto.metadata;

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.discovery.client.ServiceType;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.InternalCommunicationConfig.CommunicationProtocol;
import com.facebook.presto.server.thrift.ThriftServerInfoClient;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.statusservice.NodeStatusService;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public final class DiscoveryNodeManager
        implements InternalNodeManager
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);

    private static final Splitter CONNECTOR_ID_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final ServiceSelector serviceSelector;
    private final FailureDetector failureDetector;
    private final Optional<NodeStatusService> nodeStatusService;
    private final NodeVersion expectedNodeVersion;
    private final ConcurrentHashMap<String, RemoteNodeState> nodeStates = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final DriftClient<ThriftServerInfoClient> driftClient;
    private final ScheduledExecutorService nodeStateUpdateExecutor;
    private final ExecutorService nodeStateEventExecutor;
    private final boolean httpsRequired;
    private final InternalNode currentNode;
    private final CommunicationProtocol protocol;

    @GuardedBy("this")
    private SetMultimap<ConnectorId, InternalNode> activeNodesByConnectorId;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private Set<InternalNode> coordinators;

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public DiscoveryNodeManager(
            @ServiceType("presto") ServiceSelector serviceSelector,
            NodeInfo nodeInfo,
            FailureDetector failureDetector,
            Optional<NodeStatusService> nodeStatusService,
            NodeVersion expectedNodeVersion,
            @ForNodeManager HttpClient httpClient,
            @ForNodeManager DriftClient<ThriftServerInfoClient> driftClient,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.nodeStatusService = requireNonNull(nodeStatusService, "nodeStatusService is null");
        this.expectedNodeVersion = requireNonNull(expectedNodeVersion, "expectedNodeVersion is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.driftClient = requireNonNull(driftClient, "driftClient is null");
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("node-state-poller-%s"));
        this.nodeStateEventExecutor = newCachedThreadPool(threadsNamed("node-state-events-%s"));
        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();

        this.currentNode = findCurrentNode(
                serviceSelector.selectAllServices(),
                requireNonNull(nodeInfo, "nodeInfo is null").getNodeId(),
                expectedNodeVersion,
                httpsRequired);
        this.protocol = internalCommunicationConfig.getServerInfoCommunicationProtocol();

        refreshNodesInternal();
    }

    private static InternalNode findCurrentNode(List<ServiceDescriptor> allServices, String currentNodeId, NodeVersion expectedNodeVersion, boolean httpsRequired)
    {
        for (ServiceDescriptor service : allServices) {
            URI uri = getHttpUri(service, httpsRequired);
            OptionalInt thriftPort = getThriftServerPort(service);
            NodeVersion nodeVersion = getNodeVersion(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, thriftPort, nodeVersion, isCoordinator(service));

                if (node.getNodeIdentifier().equals(currentNodeId)) {
                    checkState(
                            node.getNodeVersion().equals(expectedNodeVersion),
                            "INVARIANT: current node version (%s) should be equal to %s",
                            node.getNodeVersion(),
                            expectedNodeVersion);
                    return node;
                }
            }
        }
        throw new IllegalStateException("INVARIANT: current node not returned from service selector");
    }

    @PostConstruct
    public void startPollingNodeStates()
    {
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollWorkers();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
        pollWorkers();
    }

    private void pollWorkers()
    {
        AllNodes allNodes = getAllNodes();
        Set<InternalNode> aliveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(allNodes.getActiveNodes())
                .addAll(allNodes.getShuttingDownNodes())
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = difference(nodeStates.keySet(), aliveNodeIds).immutableCopy();
        nodeStates.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            switch (protocol) {
                case HTTP:
                    nodeStates.putIfAbsent(node.getNodeIdentifier(),
                            new HttpRemoteNodeState(httpClient, uriBuilderFrom(node.getInternalUri()).appendPath("/v1/info/state").build()));
                    break;
                case THRIFT:
                    if (node.getThriftPort().isPresent()) {
                        nodeStates.put(node.getNodeIdentifier(),
                                new ThriftRemoteNodeState(driftClient, uriBuilderFrom(node.getInternalUri()).scheme("thrift").port(node.getThriftPort().getAsInt()).build()));
                    }
                    else {
                        // thrift port has not yet been populated; ignore the node for now
                    }
                    break;
            }
        }

        // Schedule refresh
        nodeStates.values().forEach(RemoteNodeState::asyncRefresh);

        // update indexes
        refreshNodesInternal();
    }

    @PreDestroy
    public void stop()
    {
        nodeStateUpdateExecutor.shutdownNow();
    }

    @Override
    public void refreshNodes()
    {
        refreshNodesInternal();
    }

    private synchronized void refreshNodesInternal()
    {
        // This is currently a blacklist.
        // TODO: make it a whitelist (a failure-detecting service selector) and maybe build in support for injecting this in airlift
        Set<ServiceDescriptor> services = serviceSelector.selectAllServices().stream()
                .filter(service -> !failureDetector.getFailed().contains(service))
                // Allowing coordinator node in the list of services, even if it's not allowed by nodeStatusService with currentNode check
                .filter(service -> !nodeStatusService.isPresent() || nodeStatusService.get().isAllowed(service.getNodeId()) || isCoordinator(service))
                .collect(toImmutableSet());

        ImmutableSet.Builder<InternalNode> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> coordinatorsBuilder = ImmutableSet.builder();
        ImmutableSetMultimap.Builder<ConnectorId, InternalNode> byConnectorIdBuilder = ImmutableSetMultimap.builder();

        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service, httpsRequired);
            OptionalInt thriftPort = getThriftServerPort(service);
            NodeVersion nodeVersion = getNodeVersion(service);
            boolean coordinator = isCoordinator(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, thriftPort, nodeVersion, coordinator);
                NodeState nodeState = getNodeState(node);

                switch (nodeState) {
                    case ACTIVE:
                        activeNodesBuilder.add(node);
                        if (coordinator) {
                            coordinatorsBuilder.add(node);
                        }

                        // record available active nodes organized by connector id
                        String connectorIds = service.getProperties().get("connectorIds");
                        if (connectorIds != null) {
                            connectorIds = connectorIds.toLowerCase(ENGLISH);
                            for (String connectorId : CONNECTOR_ID_SPLITTER.split(connectorIds)) {
                                byConnectorIdBuilder.put(new ConnectorId(connectorId), node);
                            }
                        }

                        // always add system connector
                        byConnectorIdBuilder.put(new ConnectorId(GlobalSystemConnector.NAME), node);
                        break;
                    case INACTIVE:
                        inactiveNodesBuilder.add(node);
                        break;
                    case SHUTTING_DOWN:
                        shuttingDownNodesBuilder.add(node);
                        break;
                    default:
                        log.error("Unknown state %s for node %s", nodeState, node);
                }
            }
        }

        if (allNodes != null) {
            // log node that are no longer active (but not shutting down)
            SetView<InternalNode> missingNodes = difference(allNodes.getActiveNodes(), Sets.union(activeNodesBuilder.build(), shuttingDownNodesBuilder.build()));
            for (InternalNode missingNode : missingNodes) {
                log.info("Previously active node is missing: %s (last seen at %s)", missingNode.getNodeIdentifier(), missingNode.getHost());
            }
        }

        // nodes by connector id changes anytime a node adds or removes a connector (note: this is not part of the listener system)
        activeNodesByConnectorId = byConnectorIdBuilder.build();

        AllNodes allNodes = new AllNodes(activeNodesBuilder.build(), inactiveNodesBuilder.build(), shuttingDownNodesBuilder.build(), coordinatorsBuilder.build());
        // only update if all nodes actually changed (note: this does not include the connectors registered with the nodes)
        if (!allNodes.equals(this.allNodes)) {
            // assign allNodes to a local variable for use in the callback below
            this.allNodes = allNodes;
            coordinators = coordinatorsBuilder.build();

            // notify listeners
            List<Consumer<AllNodes>> listeners = ImmutableList.copyOf(this.listeners);
            nodeStateEventExecutor.submit(() -> listeners.forEach(listener -> listener.accept(allNodes)));
        }
    }

    private NodeState getNodeState(InternalNode node)
    {
        if (expectedNodeVersion.equals(node.getNodeVersion())) {
            if (isNodeShuttingDown(node.getNodeIdentifier())) {
                return SHUTTING_DOWN;
            }
            else {
                return ACTIVE;
            }
        }
        else {
            return INACTIVE;
        }
    }

    private boolean isNodeShuttingDown(String nodeId)
    {
        Optional<NodeState> remoteNodeState = nodeStates.containsKey(nodeId)
                ? nodeStates.get(nodeId).getNodeState()
                : Optional.empty();
        return remoteNodeState.isPresent() && remoteNodeState.get() == SHUTTING_DOWN;
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        return allNodes;
    }

    @Managed
    public int getActiveNodeCount()
    {
        return getAllNodes().getActiveNodes().size();
    }

    @Managed
    public int getInactiveNodeCount()
    {
        return getAllNodes().getInactiveNodes().size();
    }

    @Managed
    public int getShuttingDownNodeCount()
    {
        return getAllNodes().getShuttingDownNodes().size();
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        switch (state) {
            case ACTIVE:
                return getAllNodes().getActiveNodes();
            case INACTIVE:
                return getAllNodes().getInactiveNodes();
            case SHUTTING_DOWN:
                return getAllNodes().getShuttingDownNodes();
            default:
                throw new IllegalArgumentException("Unknown node state " + state);
        }
    }

    @Override
    public synchronized Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
    {
        return activeNodesByConnectorId.get(connectorId);
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return currentNode;
    }

    @Override
    public synchronized Set<InternalNode> getCoordinators()
    {
        return coordinators;
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        AllNodes allNodes = this.allNodes;
        nodeStateEventExecutor.submit(() -> listener.accept(allNodes));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }

    private static URI getHttpUri(ServiceDescriptor descriptor, boolean httpsRequired)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }

    private static OptionalInt getThriftServerPort(ServiceDescriptor descriptor)
    {
        String port = descriptor.getProperties().get("thriftServerPort");
        if (port != null) {
            try {
                return OptionalInt.of(Integer.parseInt(port));
            }
            catch (IllegalArgumentException ignored) {
            }
        }
        return OptionalInt.empty();
    }

    private static NodeVersion getNodeVersion(ServiceDescriptor descriptor)
    {
        String nodeVersion = descriptor.getProperties().get("node_version");
        return nodeVersion == null ? null : new NodeVersion(nodeVersion);
    }

    private static boolean isCoordinator(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("coordinator"));
    }
}
