/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ser.ToStringSerializer;

import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class HiveClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        jsonBinder(binder).addSerializerBinding(Path.class).toInstance(ToStringSerializer.instance);
        jsonBinder(binder).addDeserializerBinding(Path.class).toInstance(PathJsonDeserializer.INSTANCE);
        jsonCodecBinder(binder).bindJsonCodec(HivePartitionChunk.class);
    }
}
