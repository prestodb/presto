package com.facebook.presto.split;

import com.facebook.presto.hive.CachingHiveClient;
import com.facebook.presto.hive.HiveMetadataCache;
import com.facebook.presto.spi.ImportClient;
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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.MBeanExporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ImportClientFactory
{
    private final ServiceSelector selector;
    private final DataSize maxChunkSize;

    private final Cache<String, MetadataCache> metadataCaches;
    private final MBeanExporter mbeanExporter;

    @Inject
    public ImportClientFactory(@ServiceType("hive-metastore") ServiceSelector selector, HiveClientConfig hiveClientConfig, MBeanExporter mbeanExporter)
    {
        this.selector = selector;
        this.maxChunkSize = hiveClientConfig.getMaxChunkSize();
        this.metadataCaches = CacheBuilder.newBuilder().build();
        this.mbeanExporter = mbeanExporter;
    }

    // TODO: includes hack to support presto installations supporting multiple hive dbs
    public ImportClient getClient(String sourceName)
    {
        checkArgument(sourceName.startsWith("hive_"), "bad source name: %s", sourceName);

        final String metastoreName = sourceName.split("_", 2)[1];
        checkArgument(!metastoreName.isEmpty(), "bad metastore name: %s", metastoreName);

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

        HostAndPort metastore = shuffle(metastores).get(0);
        return new CachingHiveClient(metastore.getHostText(), metastore.getPort(), metadataCache, maxChunkSize.toBytes());
    }
}
