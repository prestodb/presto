package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.operation.LongLessThanComparison;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.createLongsBlock;
import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextValue;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static org.testng.Assert.assertTrue;

public class TestComparisonOperator
{
    @Test
    public void testAligned()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 2, 3, 4, 5),
                createLongsBlock(5, 6, 7, 8, 9, 10)));

        TupleStream right = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 0, 2, 5, 3),
                createLongsBlock(5, 6, 8, 6, 11, 4)));

        ComparisonOperator operator = new ComparisonOperator(left, right, new LongLessThanComparison());

        Cursor cursor = operator.cursor(new QuerySession());

        assertNextPosition(cursor, 3);
        assertNextPosition(cursor, 6);
        assertNextPosition(cursor, 8);

        assertAdvanceNextPosition(cursor, FINISHED);
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testUnaligned()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 2, 3, 4, 5),
                createLongsBlock(5, 6, 7, 8, 9, 10)));

        TupleStream right = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 0, 3, 3, 6, 8, 4, 8, 10),
                createLongsBlock(8, 5, 20)));

        ComparisonOperator operator = new ComparisonOperator(left, right, new LongLessThanComparison());

        Cursor cursor = operator.cursor(new QuerySession());

        assertNextPosition(cursor, 1);
        assertNextPosition(cursor, 3);
        assertNextPosition(cursor, 4);
        assertNextPosition(cursor, 6);
        assertNextPosition(cursor, 7);
        assertNextPosition(cursor, 9);

        assertAdvanceNextValue(cursor, FINISHED);
        assertAdvanceNextPosition(cursor, FINISHED);
    }
}
