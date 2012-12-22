/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;

public class ImportPartition
        implements RecordIterable
{
    private final ImportClient importClient;
    private final PartitionChunk chunk;
    private final int fieldCount;

    public ImportPartition(ImportClient importClient, PartitionChunk chunk, int fieldCount)
    {
        this.fieldCount = fieldCount;
        Preconditions.checkNotNull(importClient, "importClient is null");
        Preconditions.checkNotNull(chunk, "chunk is null");

        this.importClient = importClient;
        this.chunk = chunk;
    }

    public PartitionChunk getChunk()
    {
        return chunk;
    }

    @Override
    public RecordIterator iterator(OperatorStats operatorStats)
    {
        com.facebook.presto.spi.RecordIterator records = runWithRetryUnchecked(new Callable<com.facebook.presto.spi.RecordIterator>()
        {
            @Override
            public com.facebook.presto.spi.RecordIterator call()
                    throws Exception
            {
                return importClient.getRecords(chunk);
            }
        });
        operatorStats.addExpectedDataSize(chunk.getLength());
        return new ImportRecordIterator(records, fieldCount);
    }

    private static class ImportRecordIterator
            extends AbstractIterator<Record>
            implements RecordIterator
    {
        private final com.facebook.presto.spi.RecordIterator importRecords;
        private final int fieldCount;

        private ImportRecordIterator(com.facebook.presto.spi.RecordIterator importRecords, int fieldCount)
        {
            this.importRecords = importRecords;
            this.fieldCount = fieldCount;
        }

        @Override
        protected Record computeNext()
        {
            if (importRecords.hasNext()) {
                return new ImportRecord(importRecords.next(), fieldCount);
            }
            return endOfData();
        }

        @Override
        public void close()
        {
            Closeables.closeQuietly(importRecords);
        }
    }

    public static class ImportRecord
            implements Record
    {
        private final com.facebook.presto.spi.Record importRecord;
        private final int fieldCount;

        public ImportRecord(com.facebook.presto.spi.Record importRecord, int fieldCount)
        {
            this.importRecord = importRecord;
            this.fieldCount = fieldCount;
        }

        @Override
        public int getFieldCount()
        {
            return fieldCount;
        }

        @Override
        public long getLong(int field)
        {
            return importRecord.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            return importRecord.getDouble(field);
        }

        @Override
        public byte[] getString(int field)
        {
            return importRecord.getString(field);
        }

        @Override
        public boolean isNull(int field)
        {
            return importRecord.isNull(field);
        }
    }
}
