/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
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
    public RecordCursor cursor(OperatorStats operatorStats)
    {
        com.facebook.presto.spi.RecordCursor records = retry().stopOnIllegalExceptions().runUnchecked(new Callable<com.facebook.presto.spi.RecordCursor>()
        {
            @Override
            public com.facebook.presto.spi.RecordCursor call()
                    throws Exception
            {
                return importClient.getRecords(chunk, columns);
            }
        });
        return new ImportRecordCursor(records);
    }

    private static class ImportRecordCursor
            implements RecordCursor
    {
        private final com.facebook.presto.spi.RecordCursor cursor;

        private ImportRecordCursor(com.facebook.presto.spi.RecordCursor cursor)
        {
            this.cursor = cursor;
        }

        @Override
        public long getTotalBytes()
        {
            return cursor.getTotalBytes();
        }

        @Override
        public long getCompletedBytes()
        {
            return cursor.getCompletedBytes();
        }

        @Override
        public boolean advanceNextPosition()
        {
            return cursor.advanceNextPosition();
        }

        @Override
        public long getLong(int field)
        {
            return cursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            return cursor.getDouble(field);
        }

        @Override
        public byte[] getString(int field)
        {
            return cursor.getString(field);
        }

        @Override
        public boolean isNull(int field)
        {
            return cursor.isNull(field);
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }
}
