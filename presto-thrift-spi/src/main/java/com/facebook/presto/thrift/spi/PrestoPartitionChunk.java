/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PrestoPartitionChunk
{
    private final Long estimatedLength;
    private final byte[] clientData;

    @ThriftConstructor
    public PrestoPartitionChunk(Long estimatedLength, byte[] clientData)
    {
        this.estimatedLength = estimatedLength;
        this.clientData = clientData;
    }

    @ThriftField(1)
    public Long getEstimatedLength()
    {
        return estimatedLength;
    }

    @ThriftField(2)
    public byte[] getClientData()
    {
        return clientData;
    }
}
