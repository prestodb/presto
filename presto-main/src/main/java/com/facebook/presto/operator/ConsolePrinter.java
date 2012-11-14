/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;

public class ConsolePrinter
        implements Operator
{
    public static long print(Operator source)
    {
        return print(source, new DelimitedTuplePrinter());
    }

    public static long print(Operator source, TuplePrinter printer)
    {
        ConsolePrinter consolePrinter = new ConsolePrinter(source, printer);

        int rows = 0;
        for (Page page : consolePrinter) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    private final Operator source;
    private final TuplePrinter printer;

    public ConsolePrinter(Operator source, TuplePrinter printer)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(printer, "printer is null");

        this.source = source;
        this.printer = printer;
    }

    @Override
    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return source.getTupleInfos();
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new AbstractIterator<Page>()
        {
            private final Iterator<Page> iterator = source.iterator();
            private final BlockCursor[] cursors = new BlockCursor[source.getChannelCount()];
            private final Tuple[] tuples = new Tuple[source.getChannelCount()];

            @Override
            protected Page computeNext()
            {
                if (!iterator.hasNext()) {
                    return endOfData();
                }
                Page page = iterator.next();
                for (int i = 0; i < cursors.length; i++) {
                    cursors[i] = page.getBlock(i).cursor();
                }

                for (int position = 0; position < page.getPositionCount(); position++) {
                    for (int i = 0; i < cursors.length; i++) {
                        checkState(cursors[i].advanceNextPosition());
                        tuples[i] = cursors[i].getTuple();
                    }

                    printer.print(tuples);
                }

                for (BlockCursor cursor : cursors) {
                    checkState(!cursor.advanceNextPosition());
                }
                return page;
            }
        };
    }

    public interface TuplePrinter
    {
        public void print(Tuple... tuples);
    }

    public static class DelimitedTuplePrinter implements TuplePrinter
    {
        private final Writer writer;
        private final String delimiter;

        public DelimitedTuplePrinter()
        {
            this(new OutputStreamWriter(System.out, UTF_8), "\t");
        }

        public DelimitedTuplePrinter(Writer writer, String delimiter)
        {
            this.writer = writer;
            this.delimiter = delimiter;
        }

        public void print(Tuple... tuples)
        {
            try {
                boolean first = true;
                for (Tuple tuple : tuples) {
                    if (!first) {
                        writer.write(delimiter);
                    }
                    Joiner.on(delimiter).appendTo(writer, tuple.toValues());
                    first = false;
                }
                writer.write('\n');
                writer.flush();
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

        public void print(Tuple... tuples)
        {
            try {
                int index = 0;
                for (Tuple tuple : tuples) {
                    for (Object value : tuple.toValues()) {
                        writer.write(String.valueOf(index));
                        writer.write(":\t");
                        writer.write(String.valueOf(value));
                        writer.write('\n');
                        index++;
                    }
                }
                writer.write('\n');
                writer.flush();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
