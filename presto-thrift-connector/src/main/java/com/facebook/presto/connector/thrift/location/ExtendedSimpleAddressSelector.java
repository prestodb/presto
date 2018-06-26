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
package com.facebook.presto.connector.thrift.location;

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.transport.client.Address;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

public class ExtendedSimpleAddressSelector
        implements AddressSelector<Address>
{
    private final AddressSelector<Address> delegate;

    public ExtendedSimpleAddressSelector(AddressSelector<Address> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Address> selectAddress(Optional<String> context)
    {
        if (!context.isPresent()) {
            return delegate.selectAddress(Optional.empty());
        }

        List<String> list = Splitter.on(',').splitToList(context.get());
        String value = list.get(ThreadLocalRandom.current().nextInt(list.size()));
        HostAndPort address = HostAndPort.fromString(value);
        return Optional.of(() -> address);
    }
}
