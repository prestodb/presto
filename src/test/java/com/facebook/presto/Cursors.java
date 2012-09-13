/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.collect.ImmutableList;

import java.util.List;

public final class Cursors
{
    private Cursors()
    {
    }

    public static List<Pair> toPairList(Cursor cursor)
    {
        ImmutableList.Builder<Pair> builder = ImmutableList.builder();
        while (cursor.advanceNextPosition()) {
            builder.add(new Pair(cursor.getPosition(), cursor.getTuple()));
        }
        return builder.build();
    }
}
