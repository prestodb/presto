package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class CsvFileScanner implements Iterable<ValueBlock>
{
    private final InputSupplier<InputStreamReader> inputSupplier;
    private final Splitter columnSplitter;
    private final int columnIndex;

    public CsvFileScanner(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, char columnSeparator)
    {
        this.columnIndex = columnIndex;
        Preconditions.checkNotNull(inputSupplier, "inputSupplier is null");
        this.inputSupplier = inputSupplier;
        columnSplitter = Splitter.on(columnSeparator);
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new ColumnIterator(inputSupplier, columnIndex, columnSplitter);
    }

    private static class ColumnIterator extends AbstractIterator<ValueBlock>
    {
        private long position;
        private final LineReader reader;
        private int columnIndex;
        private Splitter columnSplitter;

        public ColumnIterator(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, Splitter columnSplitter)
        {
            try {
                this.reader = new LineReader(inputSupplier.getInput());
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            this.columnIndex = columnIndex;
            this.columnSplitter = columnSplitter;
        }

        @Override
        protected ValueBlock computeNext()
        {
            String line;
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            if (line == null) {
                endOfData();
                return null;
            }
            Iterable<String> split = columnSplitter.split(line);
            String value = Iterables.get(split, columnIndex);
            return new UncompressedValueBlock(position++, value);

        }
    }
}
