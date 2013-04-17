/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportPartition
        implements RecordSet
{
    private final ImportClient importClient;
    private final PartitionChunk chunk;
    private final List<ColumnHandle> columns;

    public ImportPartition(ImportClient importClient, PartitionChunk chunk, List<? extends ColumnHandle> columns)
    {
        this.importClient = checkNotNull(importClient, "importClient is null");
        this.chunk = checkNotNull(chunk, "chunk is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
    }

    @Override
    public RecordCursor cursor()
    {
        return retry().stopOnIllegalExceptions().runUnchecked(new Callable<RecordCursor>()
        {
            @Override
            public RecordCursor call()
                    throws Exception
            {
                return importClient.getRecords(chunk, columns);
            }
        });
    }
}
