/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.thrift.spi.PrestoPartitionChunk;

class PrestoThriftPartitionChunk extends PrestoPartitionChunk implements PartitionChunk
{
    PrestoThriftPartitionChunk(PrestoPartitionChunk chunk)
    {
        super(chunk.getEstimatedLength(), chunk.getClientData());
    }

    @Override
    public long getLength()
    {
        Long estimatedLength = getEstimatedLength();

        return estimatedLength == null ? 0 : estimatedLength;
    }
}
