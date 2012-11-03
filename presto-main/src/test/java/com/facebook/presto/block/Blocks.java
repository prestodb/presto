package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class Blocks
{
    public static void assertTupleStreamEquals(TupleStream actual, TupleStream expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());
        assertCursorsEquals(actual.cursor(new QuerySession()), expected.cursor(new QuerySession()));
    }
    
    public static void assertCursorsEquals(Cursor actualCursor, Cursor expectedCursor)
    {
        Assert.assertEquals(actualCursor.getTupleInfo(), expectedCursor.getTupleInfo());
        while (advanceAllCursorsToNextPosition(actualCursor, expectedCursor)) {
            assertEquals(actualCursor.getTuple(), expectedCursor.getTuple());
        }
        assertTrue(actualCursor.isFinished());
        assertTrue(expectedCursor.isFinished());
    }

    private static boolean advanceAllCursorsToNextPosition(Cursor... cursors)
    {
        boolean allAdvanced = true;
        for (Cursor cursor : cursors) {
            allAdvanced = Cursors.advanceNextPositionNoYield(cursor) && allAdvanced;
        }
        return allAdvanced;
    }

    public static TupleStream createTupleStream(int position, String... values)
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, createBlock(position, values));
    }

    public static UncompressedBlock createBlock(long position, String... values)
    {
        return createStringBlock(position, ImmutableList.copyOf(values));
    }

    public static UncompressedBlock createStringBlock(long position, Iterable<String> values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_VARBINARY);

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }
}
