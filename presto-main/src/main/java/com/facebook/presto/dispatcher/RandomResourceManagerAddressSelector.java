package com.facebook.presto.dispatcher;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.google.common.net.HostAndPort;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;

public class RandomResourceManagerAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private final InternalNodeManager internalNodeManager;

    @Inject
    public RandomResourceManagerAddressSelector(InternalNodeManager internalNodeManager)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext)
    {
        List<HostAndPort> resourceManagers = internalNodeManager.getResourceManagers()
                .stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(resourceManagerNode -> {
                    HostAddress hostAndPort = resourceManagerNode.getHostAndPort();
                    return HostAndPort.fromParts(hostAndPort.getHostText(), resourceManagerNode.getThriftPort().getAsInt());
                })
                .collect(toImmutableList());
        if (resourceManagers.isEmpty()) {
            return Optional.empty();
        }
        HostAndPort chosenResourceManager = resourceManagers.get(ThreadLocalRandom.current().nextInt(resourceManagers.size()));
        return Optional.of(new SimpleAddress(chosenResourceManager));
    }
}
