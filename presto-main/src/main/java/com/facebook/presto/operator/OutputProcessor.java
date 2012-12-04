package com.facebook.presto.operator;

import com.facebook.presto.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.operator.OutputSink.OutputSinkHandler;
import static com.google.common.base.Preconditions.checkNotNull;

public class OutputProcessor
{
    private final OutputHandler handler;
    private final OutputSink sink;

    public OutputProcessor(Operator source, OutputHandler handler)
    {
        checkNotNull(source, "source is null");
        checkNotNull(handler, "handler is null");

        this.handler = handler;
        this.sink = new OutputSink(source, createOutputSinkHandler(handler));
    }

    public OutputStats process()
    {
        long rows = 0;
        long bytes = 0;
        for (Page page : sink) {
            rows += page.getPositionCount();
            bytes = page.getDataSize().toBytes();
        }
        handler.finish();
        return new OutputStats(rows, bytes);
    }

    public abstract static class OutputHandler
    {
        public abstract void process(List<Object> values);

        public void finish() {}
    }

    public static class OutputStats
    {
        private final long rows;
        private final long bytes;

        public OutputStats(long rows, long bytes)
        {
            this.rows = rows;
            this.bytes = bytes;
        }

        public long getRows()
        {
            return rows;
        }

        public long getBytes()
        {
            return bytes;
        }
    }

    private static OutputSinkHandler createOutputSinkHandler(final OutputHandler handler)
    {
        return new OutputSinkHandler()
        {
            @Override
            public void process(Tuple... tuples)
            {
                List<Object> list = new ArrayList<>();
                for (Tuple tuple : tuples) {
                    list.addAll(tuple.toValues());
                }
                handler.process(Collections.unmodifiableList(list));
            }
        };
    }
}
