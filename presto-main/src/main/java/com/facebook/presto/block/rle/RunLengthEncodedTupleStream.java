package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.google.common.base.Preconditions;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class RunLengthEncodedTupleStream
        implements TupleStream, Iterable<RunLengthEncodedBlock>
{
    private final TupleInfo tupleInfo;
    private final Iterable<RunLengthEncodedBlock> runLengthEncodedBlocks;
    private final Range totalRange;

    public RunLengthEncodedTupleStream(TupleInfo tupleInfo, Iterable<RunLengthEncodedBlock> runLengthEncodedBlocks)
    {
        this(tupleInfo, runLengthEncodedBlocks, Range.ALL);
    }

    public RunLengthEncodedTupleStream(TupleInfo tupleInfo,
            Iterable<RunLengthEncodedBlock> runLengthEncodedBlocks,
            Range totalRange)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(runLengthEncodedBlocks, "runLengthEncodedBlocks is null");

        this.tupleInfo = tupleInfo;
        this.runLengthEncodedBlocks = runLengthEncodedBlocks;
        this.totalRange = totalRange;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return totalRange;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new RunLengthEncodedCursor(tupleInfo, runLengthEncodedBlocks.iterator(), totalRange);
    }

    @Override
    public Iterator<RunLengthEncodedBlock> iterator()
    {
        return runLengthEncodedBlocks.iterator();
    }
}
