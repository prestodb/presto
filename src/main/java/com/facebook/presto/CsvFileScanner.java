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

import static com.google.common.base.Charsets.*;
import static com.google.common.base.Charsets.UTF_8;

public class CsvFileScanner implements Iterable<ValueBlock>
{
    private final InputSupplier<InputStreamReader> inputSupplier;
    private final Splitter columnSplitter;
    private final int columnIndex;
    private final TupleInfo.Type columnType;

    public CsvFileScanner(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, char columnSeparator, TupleInfo.Type columnType)
    {
        this.columnType = columnType;
        Preconditions.checkNotNull(inputSupplier, "inputSupplier is null");

        this.columnIndex = columnIndex;
        this.inputSupplier = inputSupplier;
        columnSplitter = Splitter.on(columnSeparator);
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new ColumnIterator(inputSupplier, columnIndex, columnSplitter, columnType);
    }

    private static class ColumnIterator extends AbstractIterator<ValueBlock>
    {
        private final LineReader reader;
        private final TupleInfo tupleInfo;
        private final int columnIndex;
        private final Splitter columnSplitter;
        private long position;
        private final TupleInfo.Type columnType;

        public ColumnIterator(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, Splitter columnSplitter, TupleInfo.Type columnType)
        {
            this.columnType = columnType;
            this.tupleInfo = new TupleInfo(columnType);
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
                if (columnType == TupleInfo.Type.FIXED_INT_64) {
                    blockBuilder.append(Long.valueOf(value));
                }
                else {
                    blockBuilder.append(value.getBytes(UTF_8));
                }

                if (blockBuilder.isFull()) {
                    break;
                }
                line = nextLine();
            } while (line != null);

            ValueBlock block = blockBuilder.build();
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
