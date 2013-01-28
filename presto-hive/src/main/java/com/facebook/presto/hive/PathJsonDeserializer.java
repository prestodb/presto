/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.deser.std.FromStringDeserializer;

import java.io.IOException;

public class PathJsonDeserializer
        extends FromStringDeserializer<Path>
{
    public static final PathJsonDeserializer INSTANCE = new PathJsonDeserializer();

    public PathJsonDeserializer()
    {
        super(Path.class);
    }

    @Override
    protected Path _deserialize(String value, DeserializationContext ctxt)
            throws IOException
    {
        return new Path(value);
    }
}
