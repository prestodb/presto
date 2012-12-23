/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;

public class InMemoryRecordSet
        implements RecordSet
{
    private final Iterable<? extends List<?>> records;

    public InMemoryRecordSet(Iterable<? extends List<?>> records)
    {
        this.records = records;
    }

    @Override
    public RecordCursor cursor(OperatorStats operatorStats)
    {
        return new InMemoryRecordCursor(records.iterator());
    }

    private static class InMemoryRecordCursor
            implements RecordCursor
    {
        private final Iterator<? extends List<?>> records;
        private List<?> record;

        private InMemoryRecordCursor(Iterator<? extends List<?>> records)
        {
            this.records = records;
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (!records.hasNext()) {
                record = null;
                return false;
            }
            record = records.next();
            return true;
        }

        @Override
        public long getLong(int field)
        {
            Preconditions.checkState(record != null, "no current record");
            Preconditions.checkNotNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            Preconditions.checkState(record != null, "no current record");
            Preconditions.checkNotNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public byte[] getString(int field)
        {
            Preconditions.checkState(record != null, "no current record");
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
            Preconditions.checkState(record != null, "no current record");
            return record.get(field) == null;
        }

        @Override
        public void close()
        {
        }
    }

}
