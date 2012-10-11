package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestFullInlinedOperatorTupleStream
{
    @Test
    public void testSanity() throws Exception
    {
        MockInlinedOperatorReader mockInlinedOperatorReader = new MockInlinedOperatorReader();
        TupleStream tupleStream = new FullInlinedOperatorTupleStream(mockInlinedOperatorReader);
        Cursor cursor1 = tupleStream.cursor(new QuerySession());
        Cursor cursor2 = tupleStream.cursor(new QuerySession());

        Assert.assertTrue(cursor1.advanceNextPosition() == Cursor.AdvanceResult.MUST_YIELD);
        Assert.assertTrue(cursor2.advanceNextPosition() == Cursor.AdvanceResult.MUST_YIELD);
        Assert.assertFalse(cursor1.isValid());
        Assert.assertFalse(cursor2.isValid());
        Assert.assertFalse(cursor1.isFinished());
        Assert.assertFalse(cursor2.isFinished());
        
        // Still no results
        Assert.assertTrue(cursor1.advanceNextPosition() == Cursor.AdvanceResult.MUST_YIELD);
        Assert.assertTrue(cursor2.advanceNextPosition() == Cursor.AdvanceResult.MUST_YIELD);
        
        mockInlinedOperatorReader.setFinished();
        Assert.assertTrue(cursor1.advanceNextPosition() == Cursor.AdvanceResult.SUCCESS);
        Assert.assertTrue(cursor2.advanceNextPosition() == Cursor.AdvanceResult.SUCCESS);
        Assert.assertTrue(cursor1.isValid());
        Assert.assertTrue(cursor2.isValid());
        Assert.assertFalse(cursor1.isFinished());
        Assert.assertFalse(cursor2.isFinished());
        Assert.assertEquals(cursor1.getLong(0), 5L);
        Assert.assertEquals(cursor2.getLong(0), 5L);
        
        Assert.assertTrue(cursor1.advanceNextPosition() == Cursor.AdvanceResult.FINISHED);
        Assert.assertTrue(cursor2.advanceNextPosition() == Cursor.AdvanceResult.FINISHED);
        Assert.assertTrue(cursor1.isFinished());
        Assert.assertTrue(cursor2.isFinished());
    }

    private static class MockInlinedOperatorReader
            implements InlinedOperatorReader
    {
        private boolean finished;

        public void setFinished()
        {
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return TupleInfo.SINGLE_LONG;
        }

        @Override
        public Range getRange()
        {
            return Range.create(0, 0);
        }

        @Override
        public TupleStream getResult()
        {
            return Blocks.createLongsTupleStream(0, 5);
        }
    }
}