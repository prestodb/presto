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
package com.facebook.presto.catalogserver;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RandomCatalogServerAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private final InternalNodeManager internalNodeManager;
    private final Function<List<HostAndPort>, Optional<HostAndPort>> hostSelector;

    @Inject
    public RandomCatalogServerAddressSelector(InternalNodeManager internalNodeManager)
    {
        this(internalNodeManager, RandomCatalogServerAddressSelector::selectRandomHost);
    }

    @VisibleForTesting
    RandomCatalogServerAddressSelector(
            InternalNodeManager internalNodeManager,
            Function<List<HostAndPort>, Optional<HostAndPort>> hostSelector)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.hostSelector = requireNonNull(hostSelector, "hostSelector is null");
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext)
    {
        if (addressSelectionContext.isPresent()) {
            return addressSelectionContext
                    .map(HostAndPort::fromString)
                    .map(SimpleAddress::new);
        }
        List<HostAndPort> catalogServers = internalNodeManager.getCatalogServers().stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(catalogServerNode -> {
                    HostAddress hostAndPort = catalogServerNode.getHostAndPort();
                    return HostAndPort.fromParts(hostAndPort.getHostText(), catalogServerNode.getThriftPort().getAsInt());
                })
                .collect(toImmutableList());
        return hostSelector.apply(catalogServers).map(SimpleAddress::new);
    }

    private static Optional<HostAndPort> selectRandomHost(List<HostAndPort> hostAndPorts)
    {
        if (hostAndPorts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(hostAndPorts.get(ThreadLocalRandom.current().nextInt(hostAndPorts.size())));
    }
}
