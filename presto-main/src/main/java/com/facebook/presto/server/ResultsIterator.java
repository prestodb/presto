package com.facebook.presto.server;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputSink;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tuple.Tuple;
import com.google.common.collect.AbstractIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.emptyIterator;
import static java.util.Collections.unmodifiableList;

public class ResultsIterator
        extends AbstractIterator<List<Object>>
{
    private final PageIterator pages;
    private List<List<Object>> rowBuffer;
    private Iterator<List<Object>> rows;

    public ResultsIterator(Operator source)
    {
        checkNotNull(source, "source is null");
        OutputSink sink = new OutputSink(source, createOutputSinkHandler());
        this.pages = sink.iterator(new OperatorStats());
        this.rows = emptyIterator();
    }

    @Override
    protected List<Object> computeNext()
    {
        if (rows.hasNext()) {
            return rows.next();
        }

        rowBuffer = new ArrayList<>();
        if (pages.hasNext()) {
            pages.next();
            rows = rowBuffer.iterator();
            return computeNext();
        }
        return endOfData();
    }

    private OutputSink.OutputSinkHandler createOutputSinkHandler()
    {
        return new OutputSink.OutputSinkHandler()
        {
            @Override
            public void process(Tuple... tuples)
            {
                List<Object> row = new ArrayList<>();
                for (Tuple tuple : tuples) {
                    row.addAll(tuple.toValues());
                }
                rowBuffer.add(unmodifiableList(row));
            }
        };
    }
}
