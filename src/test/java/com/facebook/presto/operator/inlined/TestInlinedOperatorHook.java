package com.facebook.presto.operator.inlined;

import com.facebook.presto.block.*;
import com.facebook.presto.operator.GroupByOperator;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestInlinedOperatorHook
{
    @Test
    public void testSanity() throws Exception
    {
        MockInlinedOperatorWriter mockInlinedOperatorWriter = new MockInlinedOperatorWriter();
        TupleStream tupleStream = new InlinedOperatorHook(new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b", "c", "d", "e")), mockInlinedOperatorWriter);
        Cursor cursor = tupleStream.cursor(new QuerySession());

        Assert.assertTrue(mockInlinedOperatorWriter.getPositionList().isEmpty());
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        cursor.advanceNextPosition();
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L));
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        // Only process in runs so we only list each unique run in this test
        cursor.advanceNextPosition();
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L));
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        cursor.advanceNextValue();
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L, 2L));
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        cursor.advanceNextValue();
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L, 2L, 4L));
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        // Should seek to end, but include everything in between
        cursor.advanceToPosition(6);
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L, 2L, 4L, 5L, 6L));
        Assert.assertFalse(mockInlinedOperatorWriter.isFinished());
        
        // Finished
        cursor.advanceNextPosition();
        Assert.assertEquals(mockInlinedOperatorWriter.getPositionList(), ImmutableList.of(0L, 2L, 4L, 5L, 6L));
        Assert.assertTrue(mockInlinedOperatorWriter.isFinished());
    }

    private static class MockInlinedOperatorWriter
            implements InlinedOperatorWriter
    {
        private final List<Long> positionList = new ArrayList<>();
        private boolean finished;

        @Override
        public void process(TupleStreamPosition tupleStreamPosition)
        {
            positionList.add(tupleStreamPosition.getPosition());
        }

        @Override
        public void finished()
        {
            finished = true;
        }

        public List<Long> getPositionList()
        {
            return positionList;
        }

        public boolean isFinished()
        {
            return finished;
        }
    }
}
