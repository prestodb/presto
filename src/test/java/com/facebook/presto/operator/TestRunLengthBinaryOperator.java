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
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRunLengthBinaryOperator
{
    @Test
    public void testSingleBlockRleVsRle()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        RunLengthEncodedBlockStream left = new RunLengthEncodedBlockStream(info, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(0, 4))));
        RunLengthEncodedBlockStream right = new RunLengthEncodedBlockStream(info, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(1), Range.create(0, 4))));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 2);
        assertEquals(cursor.getCurrentValueEndPosition(), 4);

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testSingleBlockUncompressedVsUncompressed()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        UncompressedBlockStream left = new UncompressedBlockStream(info, createLongsBlock(0, 10, 11, 12, 13, 14));
        UncompressedBlockStream right = new UncompressedBlockStream(info, createLongsBlock(0, 9, 8, 7, 6, 5));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 1);
        assertEquals(cursor.getCurrentValueEndPosition(), 0);

        assertNextValue(cursor, 1, 3);
        assertEquals(cursor.getCurrentValueEndPosition(), 1);

        assertNextValue(cursor, 2, 5);
        assertEquals(cursor.getCurrentValueEndPosition(), 2);

        assertNextValue(cursor, 3, 7);
        assertEquals(cursor.getCurrentValueEndPosition(), 3);

        assertNextValue(cursor, 4, 9);
        assertEquals(cursor.getCurrentValueEndPosition(), 4);

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testUnalignedRleVsRle()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        RunLengthEncodedBlockStream left = new RunLengthEncodedBlockStream(info, ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple(8), Range.create(0, 4)),
                new RunLengthEncodedBlock(Tuples.createTuple(16), Range.create(5, 9))));

        RunLengthEncodedBlockStream right = new RunLengthEncodedBlockStream(info, ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple(5), Range.create(0, 7)),
                new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(8, 9))));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 3);
        assertEquals(cursor.getCurrentValueEndPosition(), 4);

        assertNextValue(cursor, 5, 11);
        assertEquals(cursor.getCurrentValueEndPosition(), 7);

        assertNextValue(cursor, 8, 13);
        assertEquals(cursor.getCurrentValueEndPosition(), 9);

        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testAlignedRleVsRle()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        RunLengthEncodedBlockStream left = new RunLengthEncodedBlockStream(info, ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple(8), Range.create(0, 4)),
                new RunLengthEncodedBlock(Tuples.createTuple(16), Range.create(5, 9))));

        RunLengthEncodedBlockStream right = new RunLengthEncodedBlockStream(info, ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple(5), Range.create(0, 4)),
                new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(5, 9))));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();

        assertNextValue(cursor, 0, 3);
        assertEquals(cursor.getCurrentValueEndPosition(), 4);

        assertNextValue(cursor, 5, 13);
        assertEquals(cursor.getCurrentValueEndPosition(), 9);

        assertFalse(cursor.advanceNextValue());
    }


    @Test
    public void testAlignedUncompressed()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        UncompressedBlockStream left = new UncompressedBlockStream(info, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        UncompressedBlockStream right = new UncompressedBlockStream(info, ImmutableList.of(
                createLongsBlock(0, 10, 9, 8, 7, 6),
                createLongsBlock(5, 5, 4, 3, 2, 1)));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

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
    public void testUnalignedUncompressed()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        UncompressedBlockStream left = new UncompressedBlockStream(info, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        UncompressedBlockStream right = new UncompressedBlockStream(info, ImmutableList.of(
                createLongsBlock(0, 10, 9, 8, 7, 6, 5, 4, 3),
                createLongsBlock(8, 2, 1)));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

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

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*out of sync.*")
    public void testFailsOnOutOfSync()
            throws Exception
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        RunLengthEncodedBlockStream left = new RunLengthEncodedBlockStream(info, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(1, 5))));
        RunLengthEncodedBlockStream right = new RunLengthEncodedBlockStream(info, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(1), Range.create(0, 5))));

        RunLengthBinaryOperator operator = new RunLengthBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();
        cursor.advanceNextPosition();
    }

}
