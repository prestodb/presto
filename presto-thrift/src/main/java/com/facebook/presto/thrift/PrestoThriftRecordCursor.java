/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

class PrestoThriftRecordCursor implements RecordCursor
{
    private final Iterator<Map<Integer, String>> records;
    private final long totalBytes;

    private Map<Integer, String> record;
    private long completedBytes;

    PrestoThriftRecordCursor(List<Map<Integer, String>> records)
    {
        this.records = records.iterator();

        long totalBytes = 0;

        for (Map<Integer, String> record : records) {
            for (Map.Entry<Integer, String> entry : record.entrySet()) {
                totalBytes += entry.getValue().length();
            }
        }

        this.totalBytes = totalBytes;
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!records.hasNext()) {
            return false;
        }

        if (record != null) {
            for (Map.Entry<Integer, String> entry : record.entrySet()) {
                completedBytes += entry.getValue().length();
            }
        }

        record = records.next();

        return true;
    }

    @Override
    public long getLong(int field)
    {
        return Long.parseLong(record.get(field));
    }

    @Override
    public double getDouble(int field)
    {
        return Double.parseDouble(record.get(field));
    }

    @Override
    public byte[] getString(int field)
    {
        return record.get(field).getBytes(Charsets.UTF_8);
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
