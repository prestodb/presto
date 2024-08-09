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

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;
import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.net.HostAndPort;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LeaderResourceManagerAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private final InternalNodeManager internalNodeManager;
    private final RatisClient client;

    @Inject
    public LeaderResourceManagerAddressSelector(InternalNodeManager internalNodeManager, RatisClient ratisClient)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.client = requireNonNull(ratisClient, "ratisClient is null");
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext)
    {
        if (addressSelectionContext.isPresent()) {
            return addressSelectionContext
                    .map(HostAndPort::fromString)
                    .map(SimpleAddress::new);
        }

        return internalNodeManager.getResourceManagers().stream()
                .filter(resourceManager -> resourceManager.getNodeIdentifier().equals(client.getLeader()))
                .map(resourceManager -> {
                    HostAndPort leaderHostAndPort = HostAndPort.fromParts(resourceManager.getHost(), resourceManager.getThriftPort().getAsInt());
                    return new SimpleAddress(leaderHostAndPort);
                }).findFirst();
    }
}
