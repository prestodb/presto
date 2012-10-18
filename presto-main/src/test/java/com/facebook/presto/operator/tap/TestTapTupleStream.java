package com.facebook.presto.operator.tap;

import com.facebook.presto.block.*;
import com.facebook.presto.operator.GroupByOperator;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestTapTupleStream
{
    @Test
    public void testSanity() throws Exception
    {
        MockTupleValueSink mockTupleValueSink = new MockTupleValueSink();
        TupleStream tupleStream = new Tap(new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b", "c", "d", "e")), mockTupleValueSink);
        Cursor cursor = tupleStream.cursor(new QuerySession());

        Assert.assertTrue(mockTupleValueSink.getPositionList().isEmpty());
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        cursor.advanceNextPosition();
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L));
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        // Only process in runs so we only list each unique run in this test
        cursor.advanceNextPosition();
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L));
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        cursor.advanceNextValue();
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L, 2L));
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        cursor.advanceNextValue();
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L, 2L, 4L));
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        // Should seek to end, but include everything in between
        cursor.advanceToPosition(6);
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L, 2L, 4L, 5L, 6L));
        Assert.assertFalse(mockTupleValueSink.isFinished());
        
        // Finished
        cursor.advanceNextPosition();
        Assert.assertEquals(mockTupleValueSink.getPositionList(), ImmutableList.of(0L, 2L, 4L, 5L, 6L));
        Assert.assertTrue(mockTupleValueSink.isFinished());
    }

    private static class MockTupleValueSink
            implements TupleValueSink
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
