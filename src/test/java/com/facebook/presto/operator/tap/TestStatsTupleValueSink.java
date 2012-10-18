package com.facebook.presto.operator.tap;

import com.facebook.presto.block.*;
import com.facebook.presto.operator.GroupByOperator;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestStatsTupleValueSink
{
    private StatsTupleValueSink statsTupleValueSink;
    private TupleStream tupleStream;
    private Cursor cursor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        statsTupleValueSink = new StatsTupleValueSink();
        tupleStream = new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b"));
        cursor = tupleStream.cursor(new QuerySession());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testInitFail() throws Exception
    {
        // Should throw if underlying cursor is not advanced
        statsTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
    }
    
    @Test
    public void testPartial() throws Exception
    {
        cursor.advanceNextPosition();
        statsTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
        StatsTupleValueSink.Stats stats = statsTupleValueSink.getStats();
        Assert.assertEquals(stats.getRowCount(), 2);
        Assert.assertEquals(stats.getRunsCount(), 1);
        Assert.assertEquals(stats.getMinPosition(), 0);
        Assert.assertEquals(stats.getMaxPosition(), 1);
        Assert.assertEquals(stats.getAvgRunLength(), 2);
    }
    
    @Test
    public void testFull() throws Exception
    {
        cursor.advanceNextValue();
        statsTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        statsTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        StatsTupleValueSink.Stats stats = statsTupleValueSink.getStats();
        Assert.assertEquals(stats.getRowCount(), 4);
        Assert.assertEquals(stats.getRunsCount(), 2);
        Assert.assertEquals(stats.getMinPosition(), 0);
        Assert.assertEquals(stats.getMaxPosition(), 3);
        Assert.assertEquals(stats.getAvgRunLength(), 2);
    }
}
