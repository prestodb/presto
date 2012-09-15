package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.block.rle.RunLengthEncodedBlockStream;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.Tuples;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.facebook.presto.operation.SubtractionOperation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.createLongsBlock;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;

public class TestUncompressedBinaryOperator
{
    @Test
    public void testAligned()
            throws Exception
    {
        UncompressedBlockStream left = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        UncompressedBlockStream right = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 9, 8, 7, 6),
                createLongsBlock(5, 5, 4, 3, 2, 1)));

        UncompressedBinaryOperator operator = new UncompressedBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 0);
        assertNextValue(cursor, 1, 2);
        assertNextValue(cursor, 2, 4);
        assertNextValue(cursor, 3, 6);
        assertNextValue(cursor, 4, 8);
        assertNextValue(cursor, 5, 10);
        assertNextValue(cursor, 6, 12);
        assertNextValue(cursor, 7, 14);
        assertNextValue(cursor, 8, 16);
        assertNextValue(cursor, 9, 18);

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testUnaligned()
            throws Exception
    {
        UncompressedBlockStream left = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        UncompressedBlockStream right = new UncompressedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 9, 8, 7, 6, 5, 4, 3),
                createLongsBlock(8, 2, 1)));

        UncompressedBinaryOperator operator = new UncompressedBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 0);
        assertNextValue(cursor, 1, 2);
        assertNextValue(cursor, 2, 4);
        assertNextValue(cursor, 3, 6);
        assertNextValue(cursor, 4, 8);
        assertNextValue(cursor, 5, 10);
        assertNextValue(cursor, 6, 12);
        assertNextValue(cursor, 7, 14);
        assertNextValue(cursor, 8, 16);
        assertNextValue(cursor, 9, 18);

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());
    }

    @Test(enabled = false, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*out of sync.*")
    public void testFailsOnOutOfSync()
            throws Exception
    {
        RunLengthEncodedBlockStream left = new RunLengthEncodedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(1, 5))));
        RunLengthEncodedBlockStream right = new RunLengthEncodedBlockStream(TupleInfo.SINGLE_LONG, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(1), Range.create(0, 5))));

        UncompressedBinaryOperator operator = new UncompressedBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();
        cursor.advanceNextPosition();
    }

}
