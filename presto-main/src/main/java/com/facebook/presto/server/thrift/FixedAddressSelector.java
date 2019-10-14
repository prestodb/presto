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
