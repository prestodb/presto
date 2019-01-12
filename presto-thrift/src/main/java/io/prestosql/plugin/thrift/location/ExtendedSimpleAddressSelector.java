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
package io.prestosql.plugin.thrift.location;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.address.SimpleAddressSelector.SimpleAddress;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

public class ExtendedSimpleAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private final AddressSelector<SimpleAddress> delegate;

    public ExtendedSimpleAddressSelector(AddressSelector<SimpleAddress> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> context)
    {
        return selectAddress(context, ImmutableSet.of());
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> context, Set<SimpleAddress> attempted)
    {
        if (!context.isPresent()) {
            return delegate.selectAddress(context, attempted);
        }

        List<String> list = Splitter.on(',').splitToList(context.get());
        String value = list.get(ThreadLocalRandom.current().nextInt(list.size()));
        HostAndPort address = HostAndPort.fromString(value);
        return Optional.of(new SimpleAddress(address));
    }
}
