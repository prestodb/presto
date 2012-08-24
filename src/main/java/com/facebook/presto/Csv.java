package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.TupleInfo.Type;
import static com.facebook.presto.UncompressedBlockSerde.FloatMillisUncompressedColumnWriter;
import static com.google.common.base.Charsets.UTF_8;

public class Csv
{
    public static void processCsv(InputSupplier<InputStreamReader> inputSupplier, char columnSeparator, ColumnProcessor... processors)
            throws IOException
    {
        processCsv(inputSupplier, columnSeparator, ImmutableList.copyOf(processors));
    }

    public static void processCsv(InputSupplier<InputStreamReader> inputSupplier, char columnSeparator, List<ColumnProcessor> processors)
            throws IOException
    {
        CharStreams.readLines(inputSupplier, new CsvLineProcessor(columnSeparator, processors));
    }

    private static class CsvLineProcessor implements LineProcessor<Void>
    {
        private final Splitter columnSplitter;
        private final List<ColumnHandler> handlers;

        public CsvLineProcessor(char columnSeparator, List<ColumnProcessor> processors)
        {
            ImmutableList.Builder<ColumnHandler> builder = ImmutableList.builder();
            for (ColumnProcessor processor : processors) {
                builder.add(new ColumnHandler(processor));
            }
            final List<ColumnHandler> handlers = builder.build();


            this.handlers = handlers;
            columnSplitter = Splitter.on(columnSeparator);
        }

        @Override
        public boolean processLine(String line)
                throws IOException
        {
            Iterator<ColumnHandler> iterator = handlers.iterator();
            for (String value : columnSplitter.split(line)) {
                iterator.next().handleValue(value);
            }
            return true;
        }

        @Override
        public Void getResult()
        {
            for (ColumnHandler handler : handlers) {
                handler.finish();
            }
            return null;
        }
    }

    private static class ColumnHandler
    {
        private final ColumnProcessor processor;
        private final TupleInfo tupleInfo;

        private BlockBuilder blockBuilder;
        private int position;

        private ColumnHandler(ColumnProcessor processor)
        {
            this.processor = processor;
            tupleInfo = new TupleInfo(processor.getColumnType());
            blockBuilder = new BlockBuilder(position, tupleInfo);
        }

        public void handleValue(String value)
        {
            if (blockBuilder.isFull()) {
                flushBlock();
            }

            // TODO: fix this
            if (processor instanceof FloatMillisUncompressedColumnWriter) {
                blockBuilder.append((long) Double.parseDouble(value));
            }
            else if (processor.getColumnType() == Type.FIXED_INT_64) {
                blockBuilder.append(Long.valueOf(value));
            }
            else {
                blockBuilder.append(value.getBytes(UTF_8));
            }
        }

        public void finish()
        {
            flushBlock();
            processor.finish();
        }

        private void flushBlock()
        {
            ValueBlock block = blockBuilder.build();
            position += block.getCount();
            processor.processBlock(block);
            blockBuilder = new BlockBuilder(position, tupleInfo);
        }
    }

    public static Iterable<ValueBlock> readCsvColumn(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, char columnSeparator, Type columnType)
    {
        return new CsvColumnReader(inputSupplier, columnIndex, columnSeparator, columnType);
    }

    private static class CsvColumnReader implements Iterable<ValueBlock>
    {
        private final InputSupplier<InputStreamReader> inputSupplier;
        private final Splitter columnSplitter;
        private final int columnIndex;
        private final Type columnType;

        public CsvColumnReader(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, char columnSeparator, Type columnType)
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
            private final Type columnType;

            public ColumnIterator(InputSupplier<InputStreamReader> inputSupplier, int columnIndex, Splitter columnSplitter, Type columnType)
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
                    if (columnType == Type.FIXED_INT_64) {
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
}
