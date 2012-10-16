package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.ColumnMappingCursor;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Assumes all of the provided sources are already position aligned
 */
public class MergeCursor
        implements Cursor
{
    private final List<Cursor> sources;
    private final List<Cursor> columnIndexedCursors;
    private final TupleInfo tupleInfo;

    public MergeCursor(Iterable<Cursor> sources)
    {
        Preconditions.checkNotNull(sources, "cursors is null");
        this.sources = ImmutableList.copyOf(sources);
        Preconditions.checkArgument(!this.sources.isEmpty(), "must provide at least one cursor");

        // Build combined tuple info
        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();
        for (Cursor cursor : sources) {
            types.addAll(cursor.getTupleInfo().getTypes());
        }
        this.tupleInfo = new TupleInfo(types.build());

        // Create a cursor mapping to index to each field
        ImmutableList.Builder<Cursor> indexBuilder = ImmutableList.builder();
        for (Cursor cursor : sources) {
            for (int field = 0; field < cursor.getTupleInfo().getFieldCount(); field++) {
                indexBuilder.add(new ColumnMappingCursor(cursor, field));
            }
        }
        columnIndexedCursors = indexBuilder.build();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        // Use the first source as the indicator
        return sources.get(0).getRange();
    }

    @Override
    public boolean isValid()
    {
        // Use the first source as the indicator
        return sources.get(0).isValid();
    }

    @Override
    public boolean isFinished()
    {
        // Use the first source as the indicator
        return sources.get(0).isFinished();
    }

    @Override
    public AdvanceResult advanceNextValue()
    {
        boolean resultInitialized = false;
        boolean lastResult = false;
        for (Cursor cursor : sources) {
            // Make sure to pass through each cursor on each attempt to keep them aligned
            boolean result = Cursors.advanceNextValueNoYield(cursor);
            if (resultInitialized && result != lastResult) {
                throw new IllegalStateException("Unaligned cursors");
            }
            lastResult = result;
            resultInitialized = true;
        }
        return lastResult ? AdvanceResult.SUCCESS : AdvanceResult.FINISHED;
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        boolean resultInitialized = false;
        boolean lastResult = false;
        for (Cursor cursor : sources) {
            // Make sure to pass through each cursor on each attempt to keep them aligned
            boolean result = Cursors.advanceNextPositionNoYield(cursor);
            if (resultInitialized && result != lastResult) {
                throw new IllegalStateException("Unaligned cursors");
            }
            lastResult = result;
            resultInitialized = true;
        }
        return lastResult ? AdvanceResult.SUCCESS : AdvanceResult.FINISHED;
    }

    @Override
    public AdvanceResult advanceToPosition(long position)
    {
        boolean resultInitialized = false;
        boolean lastResult = false;
        for (Cursor cursor : sources) {
            // Make sure to pass through each cursor on each attempt to keep them aligned
            boolean result = Cursors.advanceToPositionNoYield(cursor, position);
            if (resultInitialized && result != lastResult) {
                throw new IllegalStateException("Unaligned cursors");
            }
            lastResult = result;
            resultInitialized = true;
        }
        return lastResult ? AdvanceResult.SUCCESS : AdvanceResult.FINISHED;
    }

    @Override
    public Tuple getTuple()
    {
        TupleInfo.Builder builder = tupleInfo.builder();
        for (Cursor cursor : sources) {
            Cursors.appendCurrentTupleToTupleBuilder(cursor, builder);
        }
        return builder.build();
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        return columnIndexedCursors.get(field).getLong(0);
    }

    @Override
    public double getDouble(int field)
    {
        Cursors.checkReadablePosition(this);
        return columnIndexedCursors.get(field).getDouble(0);
    }

    @Override
    public Slice getSlice(int field)
    {
        Cursors.checkReadablePosition(this);
        return columnIndexedCursors.get(field).getSlice(0);
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        // Use the first source as the indicator (all cursors should be at same position)
        return sources.get(0).getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        // TODO: default to not knowing about runs, but this can be optimized in the future
        return getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        return Cursors.currentTupleEquals(this, value);
    }
}