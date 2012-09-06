package com.facebook.presto;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractColumnProcessor
        implements ColumnProcessor
{
    protected final TupleInfo.Type type;
    protected final int index;
    protected final Cursor cursor;

    private boolean finished = false;

    protected AbstractColumnProcessor(TupleInfo.Type type, int index, Cursor cursor)
    {
        checkNotNull(type, "type is null");
        checkNotNull(cursor, "cursor is null");
        checkElementIndex(index, cursor.getTupleInfo().getFieldCount());
        TupleInfo.Type cursorType = cursor.getTupleInfo().getTypes().get(index);
        checkArgument(type == cursorType, "type (%s) does not match cursor type (%s) at index (%s)", type, cursorType, index);

        this.type = type;
        this.index = index;
        this.cursor = cursor;
    }

    @Override
    public final void finish()
            throws IOException
    {
        checkState(!finished, "finish called twice");
        finished = true;
        finished();
    }

    protected abstract void finished()
            throws IOException;
}
