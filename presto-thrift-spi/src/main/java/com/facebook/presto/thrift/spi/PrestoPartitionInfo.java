/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import java.util.Map;

@ThriftStruct
public class PrestoPartitionInfo
{
    private final String name;
    private final Map<String, String> keys;

    @ThriftConstructor
    public PrestoPartitionInfo(String name, Map<String, String> keys)
    {
        this.name = name;
        this.keys = keys;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    public Map<String, String> getKeys()
    {
        return keys;
    }
}
