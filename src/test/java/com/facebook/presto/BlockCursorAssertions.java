package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.operators.BlockCursor;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BlockCursorAssertions
{
    public static void assertNextValue(BlockCursor blockCursor, long position, String value)
    {
        TupleInfo info = new TupleInfo(Type.VARIABLE_BINARY);

        Tuple tuple = info.builder()
                .append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)))
                .build();

        assertTrue(blockCursor.hasNextValue());
        blockCursor.advanceNextValue();

        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertTrue(blockCursor.tupleEquals(tuple));
        assertEquals(blockCursor.getSlice(0), tuple.getSlice(0));
    }
}
