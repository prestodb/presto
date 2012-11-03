package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.google.common.base.Predicate;

import java.util.NoSuchElementException;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class Cursors
{
    public static boolean advanceNextPositionNoYield(Cursor cursor)
    {
        AdvanceResult result = cursor.advanceNextPosition();
        if (result == MUST_YIELD) {
            throw new IllegalStateException("Cursor must yield but support yield is not supported here");
        }
        return result != FINISHED;
    }

    public static boolean advanceNextValueNoYield(Cursor cursor)
    {
        AdvanceResult result = cursor.advanceNextValue();
        if (result == MUST_YIELD) {
            throw new IllegalStateException("Cursor requested caller to yield but yield is not supported here");
        }
        return result != FINISHED;
    }

    public static Predicate<Cursor> isFinished()
    {
        return new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.isFinished();
            }
        };
    }

    public static boolean currentTupleFieldEquals(Cursor cursor, Tuple tuple, int field)
    {
        checkNotNull(cursor, "cursor is null");
        checkNotNull(tuple, "tuple is null");
        checkArgument(field >= 0, "field must be greater than or equal to zero");
        TupleInfo.Type type = tuple.getTupleInfo().getTypes().get(field);
        if (!type.equals(cursor.getTupleInfo().getTypes().get(field))) {
            // Type mismatch
            return false;
        }
        switch (type) {
            case FIXED_INT_64:
                return tuple.getLong(field) == cursor.getLong(field);
            case DOUBLE:
                return tuple.getDouble(field) == cursor.getDouble(field);
            case VARIABLE_BINARY:
                return tuple.getSlice(field).equals(cursor.getSlice(field));
            default:
                throw new AssertionError("Unknown type: " + type);
        }
    }

    /**
     * Generic implementation of currentTupleEquals that is fairly efficient in most case.
     * TODO: this can probably be used to replace a lot of the lesser efficient currentTupleEquals() implementations in some cursors
     */
    public static boolean currentTupleEquals(Cursor cursor, Tuple tuple)
    {
        checkNotNull(cursor, "cursor is null");
        checkNotNull(tuple, "tuple is null");
        if (cursor.getTupleInfo().getFieldCount() != tuple.getTupleInfo().getFieldCount()) {
            return false;
        }
        for (int field = 0; field < cursor.getTupleInfo().getFieldCount(); field++) {
            if (!currentTupleFieldEquals(cursor, tuple, field)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate whether the given cursor is currently at a readable position
     */
    public static void checkReadablePosition(Cursor cursor)
    {
        if (cursor.isFinished()) {
            throw new NoSuchElementException("already finished");
        }
        checkState(cursor.isValid(), "cursor not yet advanced");
    }
}
