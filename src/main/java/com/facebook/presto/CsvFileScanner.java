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

public class CsvFileScanner implements Iterable<UncompressedValueBlock>
{
    private final InputSupplier<InputStreamReader> inputSupplier;
    private final Splitter columnSplitter;
    private final int columnIndex;
    private final TupleInfo tupleInfo;

    public CsvFileScanner(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, char columnSeparator, TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(inputSupplier, "inputSupplier is null");

        this.columnIndex = columnIndex;
        this.tupleInfo = tupleInfo;
        this.inputSupplier = inputSupplier;
        columnSplitter = Splitter.on(columnSeparator);
    }

    @Override
    public Iterator<UncompressedValueBlock> iterator()
    {
        return new ColumnIterator(inputSupplier, columnIndex, columnSplitter, tupleInfo);
    }

    private static class ColumnIterator extends AbstractIterator<UncompressedValueBlock>
    {
        private final LineReader reader;
        private final TupleInfo tupleInfo;
        private final int columnIndex;
        private final Splitter columnSplitter;
        private long position;

        public ColumnIterator(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, Splitter columnSplitter, TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
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
        protected UncompressedValueBlock computeNext()
        {
            String line = nextLine();
            if (line == null) {
                endOfData();
                return null;
            }

            BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);
            do {
                Iterable<String> split = columnSplitter.split(line);
                String value = Iterables.get(split, columnIndex);

                // calculate final value for this group
                // todo add support for other column types
                blockBuilder.append(Long.valueOf(value));

                if (blockBuilder.isFull()) {
                    break;
                }
                line = nextLine();
            } while (line != null);

            UncompressedValueBlock block = blockBuilder.build();
            position += block.getCount();
            return block;
        }

        private String nextLine()
        {
            try {
                return reader.readLine();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
