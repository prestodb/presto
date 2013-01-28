/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClientFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ser.ToStringSerializer;

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
        newSetBinder(binder, ImportClientFactory.class).addBinding().to(HiveImportClientFactory.class);
        bindConfig(binder).to(HiveClientConfig.class);
        discoveryBinder(binder).bindSelector("hive-metastore");

        jsonBinder(binder).addSerializerBinding(Path.class).toInstance(ToStringSerializer.instance);
        jsonBinder(binder).addDeserializerBinding(Path.class).toInstance(PathJsonDeserializer.INSTANCE);
        jsonCodecBinder(binder).bindJsonCodec(HivePartitionChunk.class);
    }
}
