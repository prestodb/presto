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

    public void process()
    {
        try (PageIterator pages = sink.iterator(new OperatorStats())) {
            while (pages.hasNext()) {
                pages.next();
            }
        }
        handler.finish();
    }

    public abstract static class OutputHandler
    {
        public abstract void processRow(List<?> values);

        public void finish() {}
    }

    public static void processOutput(Operator operator, OutputHandler handler)
    {
        new OutputProcessor(operator, handler).process();
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
                handler.processRow(Collections.unmodifiableList(list));
            }
        };
    }
}
