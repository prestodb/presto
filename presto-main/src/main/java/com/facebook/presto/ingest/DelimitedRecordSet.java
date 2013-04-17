/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

public class DelimitedRecordSet
        implements RecordSet
{
    private final InputSupplier<? extends Reader> readerSupplier;
    private final Splitter columnSplitter;
    private final List<Integer> columnIndexes;

    public DelimitedRecordSet(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, int... columns)
    {
        this(readerSupplier, columnSplitter, Ints.asList(columns));
    }

    public DelimitedRecordSet(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, List<Integer> columns)
    {
        Preconditions.checkNotNull(readerSupplier, "readerSupplier is null");
        Preconditions.checkNotNull(columnSplitter, "columnSplitter is null");

        this.readerSupplier = readerSupplier;
        this.columnSplitter = columnSplitter;
        this.columnIndexes = ImmutableList.copyOf(columns);
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return Collections.nCopies(columnIndexes.size(), ColumnType.STRING);
    }

    @Override
    public RecordCursor cursor()
    {
        return new DelimitedRecordCursor(readerSupplier, columnSplitter, columnIndexes);
    }

    private static class DelimitedRecordCursor
            implements RecordCursor
    {
        private final Reader reader;
        private final LineReader lineReader;
        private final Splitter columnSplitter;
        private final List<Integer> columnIndexes;
        private List<String> columns;

        private DelimitedRecordCursor(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, List<Integer> columnIndexes)
        {
            try {
                this.reader = readerSupplier.getInput();
                this.lineReader = new LineReader(reader);
                this.columnSplitter = columnSplitter;
                this.columnIndexes = columnIndexes;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
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
            int columnIndex = columnIndexes.get(field);
            return Long.parseLong(columns.get(columnIndex));
        }

        @Override
        public double getDouble(int field)
        {
            int columnIndex = columnIndexes.get(field);
            return Double.parseDouble(columns.get(columnIndex));
        }

        @Override
        public byte[] getString(int field)
        {
            int columnIndex = columnIndexes.get(field);
            return columns.get(columnIndex).getBytes(UTF_8);
        }

        @Override
        public boolean isNull(int field)
        {
            int columnIndex = columnIndexes.get(field);
            return columns.get(columnIndex).isEmpty();
        }

        @Override
        public void close()
        {
            Closeables.closeQuietly(reader);
        }
    }
}
