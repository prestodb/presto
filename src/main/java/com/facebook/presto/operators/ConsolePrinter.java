/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operators;

import com.facebook.presto.Tuple;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import static com.google.common.base.Charsets.UTF_8;

public class ConsolePrinter
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<ValueBlock> input;
    private final TuplePrinter printer;

    public ConsolePrinter(Iterator<ValueBlock> input, TuplePrinter printer)
    {
        this.input = input;
        this.printer = printer;
    }

    @Override
    protected ValueBlock computeNext()
    {
        if (!input.hasNext()) {
            endOfData();
            return null;
        }

        ValueBlock block = input.next();
        BlockCursor blockCursor = block.blockCursor();
        while (blockCursor.hasNextValue()) {
            blockCursor.advanceNextValue();
            printer.print(blockCursor.getTuple());
            while (blockCursor.hasNextValuePosition()) {
                 blockCursor.advanceNextValuePosition();
                printer.print(blockCursor.getTuple());
            }
        }
        return block;
    }

    public interface TuplePrinter
    {
        public void print(Tuple tuple);
    }

    public static class DelimitedTuplePrinter implements TuplePrinter
    {
        private final Writer writer;
        private final String delimiter;

        public DelimitedTuplePrinter()
        {
            writer = new OutputStreamWriter(System.out, UTF_8);
            delimiter = "\t";
        }

        public DelimitedTuplePrinter(Writer writer, String delimiter)
        {
            this.writer = writer;
            this.delimiter = delimiter;
        }

        public void print(Tuple tuple)
        {
            try {
                Joiner.on(delimiter).appendTo(writer, tuple.toValues());
                writer.write('\n');
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class RecordTuplePrinter implements TuplePrinter
    {
        private final Writer writer;

        public RecordTuplePrinter()
        {
            writer = new OutputStreamWriter(System.out, UTF_8);
        }

        public RecordTuplePrinter(Writer writer)
        {
            this.writer = writer;
        }

        public void print(Tuple tuple)
        {
            try {
                int index = 0;
                for (Object value : tuple.toValues()) {
                    writer.write(String.valueOf(index));
                    writer.write(":\t");
                    writer.write(String.valueOf(value));
                    writer.write('\n');
                    index++;
                }
                writer.write('\n');
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
