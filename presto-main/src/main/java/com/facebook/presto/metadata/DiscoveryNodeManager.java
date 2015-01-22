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

import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.spi.Node;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;

@ThreadSafe
public final class DiscoveryNodeManager
        implements InternalNodeManager
{
    private static final Duration MAX_AGE = new Duration(5, TimeUnit.SECONDS);

    private static final Splitter DATASOURCES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final ServiceSelector serviceSelector;
    private final NodeInfo nodeInfo;
    private final FailureDetector failureDetector;
    private final NodeVersion expectedNodeVersion;

    @GuardedBy("this")
    private SetMultimap<String, Node> activeNodesByDataSource;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private long lastUpdateTimestamp;

    @GuardedBy("this")
    private PrestoNode currentNode;

    @GuardedBy("this")
    private Set<Node> coordinators;

    @Inject
    public DiscoveryNodeManager(@ServiceType("presto") ServiceSelector serviceSelector, NodeInfo nodeInfo, FailureDetector failureDetector, NodeVersion expectedNodeVersion)
    {
        this.serviceSelector = checkNotNull(serviceSelector, "serviceSelector is null");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.failureDetector = checkNotNull(failureDetector, "failureDetector is null");
        this.expectedNodeVersion = checkNotNull(expectedNodeVersion, "expectedNodeVersion is null");

        refreshNodes();
    }

    @Override
    public synchronized void refreshNodes()
    {
        lastUpdateTimestamp = System.nanoTime();

        // This is currently a blacklist.
        // TODO: make it a whitelist (a failure-detecting service selector) and maybe build in support for injecting this in airlift
        Set<ServiceDescriptor> services = serviceSelector.selectAllServices().stream()
                .filter(service -> !failureDetector.getFailed().contains(service))
                .collect(toImmutableSet());

        // reset current node
        currentNode = null;

        ImmutableSet.Builder<Node> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<Node> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<Node> coordinatorsBuilder = ImmutableSet.builder();
        ImmutableSetMultimap.Builder<String, Node> byDataSourceBuilder = ImmutableSetMultimap.builder();

        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service);
            NodeVersion nodeVersion = getNodeVersion(service);
            if (uri != null && nodeVersion != null) {
                PrestoNode node = new PrestoNode(service.getNodeId(), uri, nodeVersion);

                // record current node
                if (node.getNodeIdentifier().equals(nodeInfo.getNodeId())) {
                    currentNode = node;
                    checkState(currentNode.getNodeVersion().equals(expectedNodeVersion), "INVARIANT: current node version should be equal to expected node version");
                }

                if (isActive(node)) {
                    activeNodesBuilder.add(node);
                    if (Boolean.parseBoolean(service.getProperties().get("coordinator"))) {
                        coordinatorsBuilder.add(node);
                    }

                    // record available active nodes organized by data source
                    String dataSources = service.getProperties().get("datasources");
                    if (dataSources != null) {
                        dataSources = dataSources.toLowerCase(ENGLISH);
                        for (String dataSource : DATASOURCES_SPLITTER.split(dataSources)) {
                            byDataSourceBuilder.put(dataSource, node);
                        }
                    }

                    // always add system data source
                    byDataSourceBuilder.put(SystemTablesManager.CONNECTOR_ID, node);
                }
                else {
                    inactiveNodesBuilder.add(node);
                }
            }
        }

        allNodes = new AllNodes(activeNodesBuilder.build(), inactiveNodesBuilder.build());
        activeNodesByDataSource = byDataSourceBuilder.build();
        coordinators = coordinatorsBuilder.build();

        checkState(currentNode != null, "INVARIANT: current node not returned from service selector");
    }

    private synchronized void refreshIfNecessary()
    {
        if (Duration.nanosSince(lastUpdateTimestamp).compareTo(MAX_AGE) > 0) {
            refreshNodes();
        }
    }

    private boolean isActive(PrestoNode node)
    {
        return expectedNodeVersion.equals(node.getNodeVersion());
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        refreshIfNecessary();
        return allNodes;
    }

    @Override
    public Set<Node> getActiveNodes()
    {
        return getAllNodes().getActiveNodes();
    }

    @Override
    public synchronized Set<Node> getActiveDatasourceNodes(String datasourceName)
    {
        refreshIfNecessary();
        return activeNodesByDataSource.get(datasourceName);
    }

    @Override
    public synchronized Node getCurrentNode()
    {
        refreshIfNecessary();
        return currentNode;
    }

    @Override
    public synchronized Set<Node> getCoordinators()
    {
        refreshIfNecessary();
        return coordinators;
    }

    private static URI getHttpUri(ServiceDescriptor descriptor)
    {
        // favor https over http
        for (String type : asList("https", "http")) {
            String url = descriptor.getProperties().get(type);
            if (url != null) {
                try {
                    return new URI(url);
                }
                catch (URISyntaxException ignored) {
                }
            }
        }
        return null;
    }

    private static NodeVersion getNodeVersion(ServiceDescriptor descriptor)
    {
        String nodeVersion = descriptor.getProperties().get("node_version");
        return nodeVersion == null ? null : new NodeVersion(nodeVersion);
    }
}
