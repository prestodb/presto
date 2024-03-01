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
package com.facebook.presto.server.thrift;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class FixedAddressSelector
        implements AddressSelector<SimpleAddress>

{
    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext)
    {
        return selectAddress(addressSelectionContext, ImmutableSet.of());
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext, Set<SimpleAddress> attempted)
    {
        checkArgument(addressSelectionContext.isPresent());

        // TODO: We should make context generic type in Drift library to avoid parsing and create address every time
        HostAndPort address = HostAndPort.fromString(addressSelectionContext.get());
        return Optional.of(new SimpleAddress(address));
    }
}
