/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.hive.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

public class HivePartition implements RecordIterable
{
    private final HiveClient hiveClient;
    private final PartitionChunk chunk;
    private final List<String> columnNames;

    public HivePartition(HiveClient hiveClient, PartitionChunk chunk, Iterable<String> columnNames)
    {
        Preconditions.checkNotNull(hiveClient, "hiveClient is null");
        Preconditions.checkNotNull(chunk, "chunk is null");

        this.hiveClient = hiveClient;
        this.chunk = chunk;
        this.columnNames = ImmutableList.copyOf(columnNames);
    }

    @Override
    public RecordIterator iterator()
    {
        return new HiveRecordIterator(hiveClient.getRecords(chunk), columnNames);
    }

    private static class HiveRecordIterator extends AbstractIterator<Record> implements RecordIterator
    {
        private final com.facebook.presto.hive.RecordIterator hiveRecords;
        private final List<String> columnNames;

        private HiveRecordIterator(com.facebook.presto.hive.RecordIterator hiveRecords, List<String> columnNames)
        {
            this.hiveRecords = hiveRecords;
            this.columnNames = columnNames;
        }

        @Override
        protected Record computeNext()
        {
            if (hiveRecords.hasNext()) {
                return new HiveRecord(hiveRecords.next(), columnNames);
            }
            return endOfData();
        }

        @Override
        public void close()
                throws IOException
        {
            hiveRecords.close();
        }
    }

    public static class HiveRecord implements Record
    {
        private final com.facebook.presto.hive.Record hiveRecord;
        private final List<String> columnNames;

        public HiveRecord(com.facebook.presto.hive.Record hiveRecord, List<String> columnNames)
        {
            this.hiveRecord = hiveRecord;
            this.columnNames = columnNames;
        }

        @Override
        public int getFieldCount()
        {
            return columnNames.size() ;
        }

        @Override
        public long getLong(int field)
        {
            return hiveRecord.getLong(columnNames.get(field));
        }

        @Override
        public double getDouble(int field)
        {
            return hiveRecord.getDouble(columnNames.get(field));
        }

        @Override
        public String getString(int field)
        {
            return hiveRecord.getString(columnNames.get(field));
        }

        @Override
        public boolean isNull(int field)
        {
            return hiveRecord.getString(columnNames.get(field)) == null;
        }
    }
}
