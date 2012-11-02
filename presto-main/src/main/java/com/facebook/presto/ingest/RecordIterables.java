/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

public final class RecordIterables
{
    private RecordIterables()
    {
    }

    public static RecordIterable asRecordIterable(Record... records)
    {
        return asRecordIterable(ImmutableList.copyOf(records));
    }

    public static RecordIterable asRecordIterable(Iterable<? extends Record> records)
    {
        return new RecordIterableAdapter(records);
    }

    private static class RecordIterableAdapter implements RecordIterable
    {

        private final Iterable<? extends Record> records;

        private RecordIterableAdapter(Iterable<? extends Record> records)
        {
            this.records = records;
        }

        @Override
        public RecordIterator iterator()
        {
            return new RecordIteratorAdapter(records.iterator());
        }
    }

    public static RecordIterator asRecordIterator(Record... records)
    {
        return asRecordIterator(ImmutableList.copyOf(records).iterator());
    }
    
    public static RecordIterator asRecordIterator(Iterator<? extends Record> records)
    {
        return new RecordIteratorAdapter(records);
    }

    private static class RecordIteratorAdapter extends ForwardingIterator<Record> implements RecordIterator
    {
        private final Iterator<? extends Record> records;

        private RecordIteratorAdapter(Iterator<? extends Record> records)
        {
            this.records = records;
        }

        @Override
        protected Iterator<Record> delegate()
        {
            return (Iterator<Record>) records;
        }

        @Override
        public void close()
        {
        }
    }
}
