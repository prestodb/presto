/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

public class DelimitedRecordSet
        implements RecordSet
{
    private final InputSupplier<? extends Reader> readerSupplier;
    private final Splitter columnSplitter;

    public DelimitedRecordSet(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter)
    {
        Preconditions.checkNotNull(readerSupplier, "readerSupplier is null");
        Preconditions.checkNotNull(columnSplitter, "columnSplitter is null");

        this.readerSupplier = readerSupplier;
        this.columnSplitter = columnSplitter;
    }

    @Override
    public RecordCursor cursor(OperatorStats operatorStats)
    {
        return new DelimitedRecordCursor(readerSupplier, columnSplitter);
    }

    private static class DelimitedRecordCursor
            implements RecordCursor
    {
        private final Reader reader;
        private final LineReader lineReader;
        private final Splitter columnSplitter;
        private List<String> columns;

        private DelimitedRecordCursor(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter)
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
        public boolean advanceNextPosition()
        {
            try {
                String line = lineReader.readLine();
                if (line == null) {
                    columns = null;
                    return false;
                }
                columns = ImmutableList.copyOf(columnSplitter.split(line));
                return true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public long getLong(int field)
        {
            return Long.parseLong(columns.get(field));
        }

        @Override
        public double getDouble(int field)
        {
            return Double.parseDouble(columns.get(field));
        }

        @Override
        public byte[] getString(int field)
        {
            return columns.get(field).getBytes(UTF_8);
        }

        @Override
        public boolean isNull(int field)
        {
            return columns.get(field).isEmpty();
        }

        @Override
        public void close()
        {
            Closeables.closeQuietly(reader);
        }
    }
}
