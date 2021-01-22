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
import com.facebook.drift.client.address.AddressSelector;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.google.common.net.HostAndPort;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;
import static java.util.Objects.requireNonNull;

public class RandomResourceManagerAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private static final Logger log = Logger.get(RandomResourceManagerAddressSelector.class);

    private final InternalNodeManager internalNodeManager;

    @Inject
    public RandomResourceManagerAddressSelector(InternalNodeManager internalNodeManager)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext)
    {
        log.error("" +
                        "Active nodes: %s\n" +
                        "Inactive nodes: %s\n" +
                        "Active RM nodes: %s\n" +
                        "Active Coordinator nodes: %s\n" +
                        "Shutting down nodes: %s\n",
                internalNodeManager.getAllNodes().getActiveNodes(),
                internalNodeManager.getAllNodes().getInactiveNodes(),
                internalNodeManager.getAllNodes().getActiveResourceManagers(),
                internalNodeManager.getAllNodes().getActiveCoordinators(),
                internalNodeManager.getAllNodes().getShuttingDownNodes());
        HostAndPort[] resourceManagers = internalNodeManager.getResourceManagers()
                .stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(resourceManagerNode -> {
                    HostAddress hostAndPort = resourceManagerNode.getHostAndPort();
                    return HostAndPort.fromParts(hostAndPort.getHostText(), resourceManagerNode.getThriftPort().getAsInt());
                })
                .toArray(HostAndPort[]::new);
        if (resourceManagers.length == 0) {
            return Optional.empty();
        }
        HostAndPort chosenResourceManager = resourceManagers[ThreadLocalRandom.current().nextInt(resourceManagers.length)];
        return Optional.of(new SimpleAddress(chosenResourceManager));
    }
}
