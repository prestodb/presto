package com.facebook.presto.block;

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;
import java.util.List;

public class Cursors
{
    /**
     * Advances all cursors to the next position

     * @return true if all cursors were advanced. Otherwise, false.
     */
    public static boolean advanceNextPosition(Iterable<Cursor> cursors)
    {
        boolean advancedAll = true;
        for (Cursor cursor : cursors) {
            advancedAll = cursor.advanceNextPosition() && advancedAll;
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
