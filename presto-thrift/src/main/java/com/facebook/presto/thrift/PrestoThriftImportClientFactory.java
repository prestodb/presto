/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.thrift.spi.PrestoImporter;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import java.util.List;
import java.util.Random;

public class PrestoThriftImportClientFactory
        implements ImportClientFactory
{
    private final ThriftClient<PrestoImporter> client;
    private final Multimap<String, HostAndPort> serviceAddresses;
    private final Random random = new Random();

    @Inject
    public PrestoThriftImportClientFactory(PrestoThriftClientConfig config, ThriftClientManager manager)
    {
        client = new ThriftClient<>(manager, PrestoImporter.class);
        serviceAddresses = config.getServiceAddresses();
    }

    @Override
    public ImportClient createClient(String sourceName)
    {
        List<HostAndPort> options = ImmutableList.copyOf(serviceAddresses.get(sourceName));

        if (options.isEmpty()) {
            return null;
        }

        HostAndPort serviceAddress = options.get(random.nextInt(options.size()));

        return new PrestoThriftImportClient(client, serviceAddress);
    }
}
