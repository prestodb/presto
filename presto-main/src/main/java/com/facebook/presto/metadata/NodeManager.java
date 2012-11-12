package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;

import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public class NodeManager
{
    private final ServiceSelector serviceSelector;

    @Inject
    public NodeManager(@ServiceType("presto") ServiceSelector serviceSelector)
    {
        this.serviceSelector = serviceSelector;
    }

    public Set<Node> getActiveNodes()
    {
        ImmutableSet.Builder<Node> nodes = ImmutableSet.builder();
        for (ServiceDescriptor descriptor : serviceSelector.selectAllServices()) {
            try {
                nodes.add(nodeFromServiceDescriptor(descriptor));
            }
            catch (IllegalArgumentException e) {
                // ignore
                // TODO: log a warning here?
            }
        }
        return nodes.build();
    }

    private static Node nodeFromServiceDescriptor(ServiceDescriptor descriptor)
    {
        URI uri = getHttpUri(descriptor);
        checkArgument(uri != null, "service descriptor is missing HTTP URI: %s", descriptor);
        return new Node(descriptor.getNodeId(), uri);
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
