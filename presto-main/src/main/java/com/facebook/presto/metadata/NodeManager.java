package com.facebook.presto.metadata;

import com.facebook.presto.server.FailureDetector;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Optional;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Arrays.asList;

@ThreadSafe
public class NodeManager
{
    private final static Duration MAX_AGE = new Duration(5, TimeUnit.SECONDS);

    private static final Splitter DATASOURCES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final ServiceSelector serviceSelector;
    private final NodeInfo nodeInfo;
    private final FailureDetector failureDetector;

    @GuardedBy("this")
    private SetMultimap<String, Node> nodesByDataSource;

    @GuardedBy("this")
    private Set<Node> allNodes;

    @GuardedBy("this")
    private long lastUpdateTimestamp;

    @GuardedBy("this")
    private Node currentNode;

    @Inject
    public NodeManager(@ServiceType("presto") ServiceSelector serviceSelector, NodeInfo nodeInfo, FailureDetector failureDetector)
    {
        this.serviceSelector = checkNotNull(serviceSelector, "serviceSelector is null");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.failureDetector = checkNotNull(failureDetector, "failureDetector is null");

        refreshNodes(true);
    }

    public synchronized void refreshNodes(boolean force)
    {
        if (force || Duration.nanosSince(lastUpdateTimestamp).compareTo(MAX_AGE) > 0) {
            lastUpdateTimestamp = System.nanoTime();

            // This is currently a blacklist.
            // TODO: make it a whitelist (a failure-detecting service selector) and maybe build in support for injecting this in airlift
            Set<ServiceDescriptor> services = IterableTransformer.on(serviceSelector.selectAllServices())
                    .select(not(in(failureDetector.getFailed())))
                    .set();

            // reset current node
            currentNode = null;

            ImmutableSet.Builder<Node> allBuilder = ImmutableSet.builder();
            ImmutableSetMultimap.Builder<String, Node> byDataSourceBuilder = ImmutableSetMultimap.builder();

            for (ServiceDescriptor service : services) {
                URI uri = getHttpUri(service);
                if (uri != null) {
                    Node node = new Node(service.getNodeId(), uri);

                    // record current node
                    if (node.getNodeIdentifier().equals(nodeInfo.getNodeId())) {
                        currentNode = node;
                    }

                    // record all available nodes
                    allBuilder.add(node);

                    // record available nodes organized by data source
                    String dataSources = service.getProperties().get("datasources");
                    if (dataSources != null) {
                        dataSources = dataSources.toLowerCase();
                        for (String dataSource : DATASOURCES_SPLITTER.split(dataSources)) {
                            byDataSourceBuilder.put(dataSource, node);
                        }
                    }
                }
            }

            allNodes = allBuilder.build();
            nodesByDataSource = byDataSourceBuilder.build();
        }
    }

    public synchronized Set<Node> getActiveNodes()
    {
        refreshNodes(false);

        return allNodes;
    }

    public synchronized Set<Node> getActiveDatasourceNodes(String datasourceName)
    {
        refreshNodes(false);

        return nodesByDataSource.get(datasourceName);
    }

    public synchronized Optional<Node> getCurrentNode()
    {
        refreshNodes(false);
        return Optional.fromNullable(currentNode);
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
}
