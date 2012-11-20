package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.spi.ImportClient;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.google.common.base.Preconditions.checkArgument;

public class ImportClientFactory
{
    private final ServiceSelector selector;

    @Inject
    public ImportClientFactory(@ServiceType("hive-metastore") ServiceSelector selector)
    {
        this.selector = selector;
    }

    public ImportClient getClient(String sourceName)
    {
        checkArgument("hive".equals(sourceName), "bad source name: %s", sourceName);

        List<ServiceDescriptor> descriptors = ImmutableList.copyOf(selector.selectAllServices());

        List<HostAndPort> metastores = new ArrayList<>();
        for (ServiceDescriptor descriptor : descriptors) {
            String thrift = descriptor.getProperties().get("thrift");
            if (thrift != null) {
                try {
                    HostAndPort metastore = HostAndPort.fromString(thrift);
                    checkArgument(metastore.hasPort());
                    metastores.add(metastore);
                }
                catch (IllegalArgumentException ignored) {
                }
            }
        }

        if (metastores.isEmpty()) {
            throw new RuntimeException("hive metastore not available: " + selector.getPool());
        }

        HostAndPort metastore = shuffle(metastores).get(0);
        return new HiveClient(metastore.getHostText(), metastore.getPort());
    }
}
