package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.operator.GroupByOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRepositioningTupleStream
{
    @Test
    public void testAdvancePositionValue()
            throws Exception
    {
        TupleStream tupleStream = new RepositioningTupleStream(
                new GroupByOperator(
                        new GenericTupleStream<>(
                                TupleInfo.SINGLE_VARBINARY,
                                Blocks.createLongsBlock(4, 0L, 1L, 1L),
                                Blocks.createLongsBlock(10, 2L, 2L, 2L),
                                Blocks.createLongsBlock(50, -1L, -2L)
                        )
                ),
                100
        );

        Cursor cursor = tupleStream.cursor(new QuerySession());
        cursor.advanceNextPosition();
        Assert.assertEquals(cursor.getLong(0), 0L);
        Assert.assertEquals(cursor.getPosition(), 100);
        cursor.advanceNextValue();
        Assert.assertEquals(cursor.getLong(0), 1L);
        Assert.assertEquals(cursor.getPosition(), 101);
        cursor.advanceNextPosition();
        Assert.assertEquals(cursor.getLong(0), 1L);
        Assert.assertEquals(cursor.getPosition(), 102);
        cursor.advanceNextValue();
        Assert.assertEquals(cursor.getLong(0), 2L);
        Assert.assertEquals(cursor.getPosition(), 103);
        cursor.advanceNextValue();
        Assert.assertEquals(cursor.getLong(0), -1L);
        Assert.assertEquals(cursor.getPosition(), 106);
        cursor.advanceNextPosition();
        Assert.assertEquals(cursor.getLong(0), -2L);
        Assert.assertEquals(cursor.getPosition(), 107);
        Assert.assertEquals(cursor.advanceNextPosition(), Cursor.AdvanceResult.FINISHED);
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        TupleStream tupleStream = new RepositioningTupleStream(
                new GroupByOperator(
                        new GenericTupleStream<>(
                                TupleInfo.SINGLE_VARBINARY,
                                Blocks.createLongsBlock(4, 0L, 1L, 1L),
                                Blocks.createLongsBlock(10, 2L, 2L, 2L),
                                Blocks.createLongsBlock(50, -1L, -2L)
                        )
                ),
                100
        );

        Cursor cursor = tupleStream.cursor(new QuerySession());
        cursor.advanceToPosition(101);
        Assert.assertEquals(cursor.getLong(0), 1L);
        Assert.assertEquals(cursor.getPosition(), 101);
        cursor.advanceToPosition(105);
        Assert.assertEquals(cursor.getLong(0), 2L);
        Assert.assertEquals(cursor.getPosition(), 105);
        cursor.advanceToPosition(106);
        Assert.assertEquals(cursor.getLong(0), -1L);
        Assert.assertEquals(cursor.getPosition(), 106);
        Assert.assertEquals(cursor.advanceToPosition(1000), Cursor.AdvanceResult.FINISHED);
    }
}
