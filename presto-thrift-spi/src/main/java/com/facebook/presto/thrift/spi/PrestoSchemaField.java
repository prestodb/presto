/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PrestoSchemaField
{
    private final String name;
    private final int id;
    private final PrestoFieldType type;
    private final boolean partitionKey;

    @ThriftConstructor
    public PrestoSchemaField(String name, int id, PrestoFieldType type, boolean partitionKey)
    {
        this.name = name;
        this.id = id;
        this.type = type;
        this.partitionKey = partitionKey;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    public int getId()
    {
        return id;
    }

    @ThriftField(3)
    public PrestoFieldType getType()
    {
        return type;
    }

    @ThriftField(4)
    public boolean isPartitionKey()
    {
        return partitionKey;
    }
}
