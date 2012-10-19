package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.LineReader;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Creates a TupleStream view on a iterator of delimited strings representing rows
 * Note: this is single pass only. Only one cursor may be created.
 */
public class DelimitedTupleStream
        extends AbstractExternalTupleStream
{
    private final Iterator<String> lineIterator;
    private final Splitter splitter;
    private String currentLine;
    private List<String> currentRowSplit;

    public DelimitedTupleStream(Iterator<String> lineIterator, Splitter splitter, TupleInfo tupleInfo)
    {
        super(tupleInfo);
        this.lineIterator = checkNotNull(lineIterator, "lineIterator is null");
        this.splitter = checkNotNull(splitter, "splitter is null");
    }

    public DelimitedTupleStream(Readable readable, Splitter splitter, TupleInfo tupleInfo)
    {
        this(new LineReaderIterator(new LineReader(checkNotNull(readable, "readable is null"))), splitter, tupleInfo);
    }

    @Override
    protected boolean computeNext()
    {
        if (!lineIterator.hasNext()) {
            return false;
        }
        currentLine = lineIterator.next();
        currentRowSplit = null;
        return true;
    }

    @Override
    protected void buildTuple(TupleInfo.Builder tupleBuilder)
    {
        int index = 0;
        for (String value : getRowSplit()) {
            tupleInfo.getTypes().get(index).convert(value, tupleBuilder);
            index++;
        }
        checkState(tupleBuilder.isComplete(), "import row schema mismatch: %s", currentLine);
    }

    @Override
    protected long getLong(int field)
    {
        return Long.valueOf(getRowSplit().get(field));
    }

    @Override
    protected double getDouble(int field)
    {
        return Double.valueOf(getRowSplit().get(field));
    }

    @Override
    protected Slice getSlice(int field)
    {
        return Slices.wrappedBuffer(getRowSplit().get(field).getBytes(Charsets.UTF_8));
    }

    private List<String> getRowSplit()
    {
        checkState(currentLine != null, "No row to split");
        if (currentRowSplit == null) {
            currentRowSplit = ImmutableList.copyOf(splitter.split(currentLine));
        }
        return currentRowSplit;
    }
}
