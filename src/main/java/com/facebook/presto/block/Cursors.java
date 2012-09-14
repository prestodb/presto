package com.facebook.presto.block;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

public class Cursors
{
    public static boolean advanceNextPosition(Iterable<Cursor> cursors)
    {
        boolean done = false;
        for (Cursor cursor : cursors) {
            done = !cursor.advanceNextPosition() || done;
        }

        return !done;
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
}
