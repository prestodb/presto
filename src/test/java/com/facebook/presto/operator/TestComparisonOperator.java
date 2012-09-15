package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.facebook.presto.operation.LongLessThanComparison;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.createLongsBlock;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestComparisonOperator
{
    @Test
    public void testAligned()
            throws Exception
    {
        UncompressedBlockStream left = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 2, 3, 4, 5),
                createLongsBlock(5, 6, 7, 8, 9, 10)));

        UncompressedBlockStream right = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 0, 2, 5, 3),
                createLongsBlock(5, 6, 8, 6, 11, 4)));

        ComparisonOperator operator = new ComparisonOperator(left, right, new LongLessThanComparison());

        Cursor cursor = operator.cursor();

        assertNextPosition(cursor, 3);
        assertNextPosition(cursor, 6);
        assertNextPosition(cursor, 8);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testUnaligned()
            throws Exception
    {
        UncompressedBlockStream left = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 1, 2, 3, 4, 5),
                createLongsBlock(5, 6, 7, 8, 9, 10)));

        UncompressedBlockStream right = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 0, 3, 3, 6, 8, 4, 8, 10),
                createLongsBlock(8, 5, 20)));

        ComparisonOperator operator = new ComparisonOperator(left, right, new LongLessThanComparison());

        Cursor cursor = operator.cursor();

        assertNextPosition(cursor, 1);
        assertNextPosition(cursor, 3);
        assertNextPosition(cursor, 4);
        assertNextPosition(cursor, 6);
        assertNextPosition(cursor, 7);
        assertNextPosition(cursor, 9);

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());
    }
}
