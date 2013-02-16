/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClientFactory;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.hadoop.fs.Path;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class HiveClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ImportClientFactory.class).addBinding().to(HiveImportClientFactory.class);
        bindConfig(binder).to(HiveClientConfig.class);
        binder.bind(HiveChunkEncoder.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindSelector("hive-metastore");

        jsonBinder(binder).addSerializerBinding(Path.class).toInstance(ToStringSerializer.instance);
        jsonBinder(binder).addDeserializerBinding(Path.class).toInstance(PathJsonDeserializer.INSTANCE);
        jsonCodecBinder(binder).bindJsonCodec(HivePartitionChunk.class);
    }
}
