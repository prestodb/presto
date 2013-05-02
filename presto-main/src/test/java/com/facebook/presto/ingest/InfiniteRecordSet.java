/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.util.List;

public class InfiniteRecordSet
        implements RecordSet
{
    private final List<ColumnType> types;
    private final List<?> record;

    public InfiniteRecordSet(List<ColumnType> types, List<?> record)
    {
        this.types = types;
        this.record = record;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InMemoryRecordCursor(record);
    }

    private static class InMemoryRecordCursor
            implements RecordCursor
    {
        private final List<?> record;

        private InMemoryRecordCursor(List<?> record)
        {
            Preconditions.checkNotNull(record, "record is null");
            this.record = record;
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public boolean advanceNextPosition()
        {
            return true;
        }

        @Override
        public long getLong(int field)
        {
            Preconditions.checkNotNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            Preconditions.checkNotNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public byte[] getString(int field)
        {
            Object value = record.get(field);
            Preconditions.checkNotNull(value, "value is null");
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            if (value instanceof String) {
                return ((String) value).getBytes(Charsets.UTF_8);
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
        }

        @Override
        public boolean isNull(int field)
        {
            return record.get(field) == null;
        }

        @Override
        public void close()
        {
        }
    }
}
