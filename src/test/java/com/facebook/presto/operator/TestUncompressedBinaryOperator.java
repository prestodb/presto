package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.Tuples;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.rle.RunLengthEncodedTupleStream;
import com.facebook.presto.operation.SubtractionOperation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.createLongsBlock;
import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextValue;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;

public class TestUncompressedBinaryOperator
{
    @Test
    public void testAligned()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        TupleStream right = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
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

        assertAdvanceNextValue(cursor, FINISHED);
        assertAdvanceNextPosition(cursor, FINISHED);
    }

    @Test
    public void testUnaligned()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
                createLongsBlock(0, 10, 11, 12, 13, 14),
                createLongsBlock(5, 15, 16, 17, 18, 19)));

        TupleStream right = new GenericTupleStream<>(TupleInfo.SINGLE_LONG, ImmutableList.of(
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

        assertAdvanceNextValue(cursor, FINISHED);
        assertAdvanceNextPosition(cursor, FINISHED);
    }

    @Test(enabled = false, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*out of sync.*")
    public void testFailsOnOutOfSync()
            throws Exception
    {
        RunLengthEncodedTupleStream left = new RunLengthEncodedTupleStream(TupleInfo.SINGLE_LONG, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(3), Range.create(1, 5))));
        RunLengthEncodedTupleStream right = new RunLengthEncodedTupleStream(TupleInfo.SINGLE_LONG, ImmutableList.of(new RunLengthEncodedBlock(Tuples.createTuple(1), Range.create(0, 5))));

        UncompressedBinaryOperator operator = new UncompressedBinaryOperator(left, right, new SubtractionOperation());

        Cursor cursor = operator.cursor();
        assertAdvanceNextPosition(cursor);
    }

}
