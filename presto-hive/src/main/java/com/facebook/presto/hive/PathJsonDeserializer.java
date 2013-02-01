/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import org.apache.hadoop.fs.Path;

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
