/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class DelimitedRecordIterable implements RecordIterable
{
    private final InputSupplier<? extends Reader> readerSupplier;
    private final Splitter columnSplitter;

    public DelimitedRecordIterable(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter)
    {
        Preconditions.checkNotNull(readerSupplier, "readerSupplier is null");
        Preconditions.checkNotNull(columnSplitter, "columnSplitter is null");

        this.readerSupplier = readerSupplier;
        this.columnSplitter = columnSplitter;
    }

    @Override
    public RecordIterator iterator()
    {
        return new DelimitedRecordIterator(readerSupplier, columnSplitter);
    }

    private static class DelimitedRecordIterator extends AbstractIterator<Record> implements RecordIterator
    {
        private final Reader reader;
        private final LineReader lineReader;
        private final Splitter columnSplitter;

        private DelimitedRecordIterator(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter)
        {
            try {
                this.reader = readerSupplier.getInput();
                this.lineReader = new LineReader(reader);
                this.columnSplitter = columnSplitter;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        protected Record computeNext()
        {
            try {
                String line = lineReader.readLine();
                if (line == null) {
                    return endOfData();
                }
                List<String> columns = ImmutableList.copyOf(columnSplitter.split(line));
                return new DelimitedRecord(columns);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            reader.close();
        }
    }

    private static class DelimitedRecord implements Record
    {
        private final List<String> columns;

        public DelimitedRecord(List<String> columns)
        {
            this.columns = columns;
        }

        @Override
        public Long getLong(int field)
        {
            return Long.parseLong(getString(field));
        }

        @Override
        public Double getDouble(int field)
        {
            return Double.parseDouble(getString(field));
        }

        @Override
        public String getString(int field)
        {
            return columns.get(field);
        }
    }
}
