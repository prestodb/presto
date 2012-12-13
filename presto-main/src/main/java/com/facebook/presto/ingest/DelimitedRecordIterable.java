/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

public class DelimitedRecordIterable
        implements RecordIterable
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
    public RecordIterator iterator(OperatorStats operatorStats)
    {
        return new DelimitedRecordIterator(readerSupplier, columnSplitter);
    }

    private static class DelimitedRecordIterator
            extends AbstractIterator<Record>
            implements RecordIterator
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
                return new StringRecord(columns);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            Closeables.closeQuietly(reader);
        }
    }
}
