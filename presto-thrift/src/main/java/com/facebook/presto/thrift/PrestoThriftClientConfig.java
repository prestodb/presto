/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;

import java.util.Map;

public class PrestoThriftClientConfig
{
    private Multimap<String, HostAndPort> serviceAddresses = ImmutableMultimap.of();

    public Multimap<String, HostAndPort> getServiceAddresses()
    {
        return serviceAddresses;
    }

    /**
     * For example: foo=localhost:1000,localhost:2000;bar=localhost:300
     */
    @Config("thrift.services")
    public PrestoThriftClientConfig setServiceAddresses(String addresses)
    {
        ImmutableMultimap.Builder<String, HostAndPort> serviceAddresses = ImmutableMultimap.builder();
        Map<String, String> addressMap = Splitter.on(";").withKeyValueSeparator("=").split(addresses);

        for (Map.Entry<String, String> entry : addressMap.entrySet()) {
            for (String address : Splitter.on(",").split(entry.getValue())) {
                serviceAddresses.put(entry.getKey(), HostAndPort.fromString(address));
            }
        }

        this.serviceAddresses = serviceAddresses.build();

        return this;
    }
}
