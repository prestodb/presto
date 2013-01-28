/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.MetadataCache;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.MBeanExporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.shuffle;

public class HiveImportClientFactory
        implements ImportClientFactory
{
    private final ServiceSelector selector;
    private final DataSize maxChunkSize;

    private final Cache<String, MetadataCache> metadataCaches;
    private final MBeanExporter mbeanExporter;
    private final JsonCodec<HivePartitionChunk> partitionChunkCodec;

    @Inject
    public HiveImportClientFactory(@ServiceType("hive-metastore") ServiceSelector selector,
            HiveClientConfig hiveClientConfig,
            JsonCodec<HivePartitionChunk> partitionChunkCodec,
            MBeanExporter mbeanExporter)
    {
        this.selector = selector;
        this.maxChunkSize = hiveClientConfig.getMaxChunkSize();
        this.metadataCaches = CacheBuilder.newBuilder().build();
        this.partitionChunkCodec = partitionChunkCodec;
        this.mbeanExporter = mbeanExporter;
    }

    @Override
    public ImportClient createClient(String sourceName)
    {
        if (!sourceName.startsWith("hive_")) {
            return null;
        }

        final String metastoreName = sourceName.split("_", 2)[1];
        checkArgument(!metastoreName.isEmpty(), "bad metastore name: %s", metastoreName);

        HostAndPort metastore = getMetaStoreAddress(metastoreName);
        HiveClient hiveClient = new HiveClient(metastore.getHostText(), metastore.getPort(), maxChunkSize.toBytes(), partitionChunkCodec);

        MetadataCache metadataCache;
        try {
            metadataCache = metadataCaches.get(metastoreName, new Callable<MetadataCache>()
            {
                @Override
                public MetadataCache call()
                        throws Exception
                {
                    String jmxName = format("com.facebook.presto:type=metadatacache,group=%s", metastoreName);
                    MetadataCache cache = new HiveMetadataCache(new Duration(60.0, TimeUnit.MINUTES)); // TODO - not fixed.
                    Map<String, Object> jmxExposed = cache.getMetadataCacheStats();
                    for (Map.Entry<String, Object> jmx : jmxExposed.entrySet()) {
                        mbeanExporter.export(format("%s,name=%s", jmxName, jmx.getKey()), jmx.getValue());
                    }
                    return cache;
                }
            });
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }

        return new CachingHiveClient(metadataCache, hiveClient);
    }

    private HostAndPort getMetaStoreAddress(String metastoreName)
    {
        List<ServiceDescriptor> descriptors = ImmutableList.copyOf(selector.selectAllServices());

        List<HostAndPort> metastores = new ArrayList<>();
        for (ServiceDescriptor descriptor : descriptors) {
            String thrift = descriptor.getProperties().get("thrift");
            String name = descriptor.getProperties().get("name");
            if (thrift != null && metastoreName.equals(name)) {
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
            throw new RuntimeException(String.format("hive metastore not available for name %s in pool %s", metastoreName, selector.getPool()));
        }
        shuffle(metastores);
        return metastores.get(0);
    }
}
