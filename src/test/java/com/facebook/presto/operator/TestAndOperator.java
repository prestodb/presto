package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import org.testng.annotations.Test;

import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertPositions;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAndOperator
{
    @Test
    public void testEqual()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 3, 4));
        TupleStream right = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 3, 4));

        AndOperator and = new AndOperator(left, right);
        Cursor cursor = and.cursor();

        assertPositions(cursor, 0, 1, 2, 3, 4);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testOverlapping()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 3, 4, 5));
        TupleStream right = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 2, 4, 5));

        AndOperator and = new AndOperator(left, right);
        Cursor cursor = and.cursor();

        assertPositions(cursor, 0, 2, 4, 5);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testDisjoint()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 3, 4));
        TupleStream right = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(5, 6, 7, 8, 9));

        AndOperator and = new AndOperator(left, right);
        Cursor cursor = and.cursor();

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMultipleStreams()
    {
        TupleStream one = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 2, 3, 4, 5));
        TupleStream two = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 4, 5));
        TupleStream three = new GenericTupleStream<>(new TupleInfo(), new UncompressedPositionBlock(0, 1, 2, 3, 5));

        AndOperator and = new AndOperator(one, two, three);
        Cursor cursor = and.cursor();

        assertPositions(cursor, 0, 2, 5);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());

    }
}
