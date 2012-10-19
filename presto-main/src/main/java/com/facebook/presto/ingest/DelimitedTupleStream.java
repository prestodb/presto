package com.facebook.presto.ingest;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.LineReader;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.*;

/**
 * Creates a TupleStream view on a iterator of delimited strings representing rows
 * Note: this is single pass only. Only one cursor may be created.
 */
public class DelimitedTupleStream
        implements TupleStream
{
    private final Iterator<String> lineIterator;
    private final Splitter splitter;
    private final TupleInfo tupleInfo;
    private boolean cursorCreated;

    public DelimitedTupleStream(Iterator<String> lineIterator, Splitter splitter, TupleInfo tupleInfo)
    {
        this.lineIterator = checkNotNull(lineIterator, "lineIterator is null");
        this.splitter = checkNotNull(splitter, "splitter is null");
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
    }

    public DelimitedTupleStream(Readable readable, Splitter splitter, TupleInfo tupleInfo)
    {
        this(new LineReaderIterator(new LineReader(checkNotNull(readable, "readable is null"))), splitter, tupleInfo);
    }


    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        checkState(!cursorCreated, "Can only create one cursor from this stream");
        cursorCreated = true;
        return new DelimitedCursor();
    }

    private class DelimitedCursor
            implements Cursor
    {
        private long currentPosition = -1;
        private String currentLine;
        private List<String> currentRowSplit;
        private boolean finished;

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Range getRange()
        {
            return Range.ALL;
        }

        @Override
        public boolean isValid()
        {
            return currentPosition >= 0 && !isFinished();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            return advanceNextPosition();
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            if (!lineIterator.hasNext()) {
                finished = true;
                return AdvanceResult.FINISHED;
            }
            currentLine = lineIterator.next();
            currentRowSplit = null;
            currentPosition++;
            return AdvanceResult.SUCCESS;
        }

        @Override
        public AdvanceResult advanceToPosition(long position)
        {
            checkArgument(position >= 0, "position must be greater than or equal to zero");
            checkArgument(position >= currentPosition, "cannot advance backwards");
            while (currentPosition < position) {
                AdvanceResult result = advanceNextPosition();
                if (result != AdvanceResult.SUCCESS) {
                    return result;
                }
            }
            return AdvanceResult.SUCCESS;
        }

        @Override
        public Tuple getTuple()
        {
            Cursors.checkReadablePosition(this);
            int index = 0;
            TupleInfo.Builder tupleBuilder = tupleInfo.builder();
            for (String value : getRowSplit()) {
                tupleInfo.getTypes().get(index).getStringValueConverter().convert(value, tupleBuilder);
                index++;
            }
            checkState(tupleBuilder.isComplete(), "import row schema mismatch: %s", currentLine);
            return tupleBuilder.build();
        }

        @Override
        public long getLong(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.FIXED_INT_64, "incorrect type");
            return Long.valueOf(getRowSplit().get(field));
        }

        @Override
        public double getDouble(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.DOUBLE, "incorrect type");
            return Double.valueOf(getRowSplit().get(field));
        }

        @Override
        public Slice getSlice(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.VARIABLE_BINARY, "incorrect type");
            return Slices.wrappedBuffer(getRowSplit().get(field).getBytes(Charsets.UTF_8));
        }

        @Override
        public long getPosition()
        {
            Cursors.checkReadablePosition(this);
            return currentPosition;
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            Cursors.checkReadablePosition(this);
            return currentPosition;
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            Cursors.checkReadablePosition(this);
            checkNotNull(value, "value is null");
            return Cursors.currentTupleEquals(this, value);
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
}
