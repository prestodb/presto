package com.facebook.presto.block;

import com.facebook.presto.block.Cursor.AdvanceResult;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;

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
            throw new IllegalStateException("Cursor must yield but support yield is not supported here");
        }
        return result != FINISHED;
    }

    public static boolean advanceToPositionNoYield(Cursor cursor, long position)
    {
        AdvanceResult result = cursor.advanceToPosition(position);
        if (result == MUST_YIELD) {
            throw new IllegalStateException("Cursor must yield but support yield is not supported here");
        }
        return result != FINISHED;
    }

    /**
     * Advances all cursors to the next position

     * @return true if all cursors were advanced. Otherwise, false.
     */
    public static boolean advanceNextPositionNoYield(Cursor... cursors)
    {
        return advanceNextPositionNoYield(ImmutableList.copyOf(cursors));
    }

    public static boolean advanceNextPositionNoYield(Iterable<Cursor> cursors)
    {
        boolean advancedAll = true;
        for (Cursor cursor : cursors) {
            advancedAll = advanceNextPositionNoYield(cursor) && advancedAll;
        }

        return advancedAll;
    }

    public static Ordering<Cursor> orderByPosition()
    {
        return new Ordering<Cursor>()
        {
            @Override
            public int compare(Cursor left, Cursor right)
            {
                return Longs.compare(left.getPosition(), right.getPosition());
            }
        };
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
}
