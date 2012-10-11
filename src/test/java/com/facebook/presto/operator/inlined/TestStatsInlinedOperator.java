package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.block.*;
import com.facebook.presto.operator.GroupByOperator;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.inlined.StatsInlinedOperator.Fields.*;

public class TestStatsInlinedOperator
{
    private StatsInlinedOperator statsInlinedOperator;
    private TupleStream tupleStream;
    private Cursor cursor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        statsInlinedOperator = new StatsInlinedOperator();
        tupleStream = new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b"));
        cursor = tupleStream.cursor(new QuerySession());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testInitFail() throws Exception
    {
        // Should throw if underlying cursor is not advanced
        statsInlinedOperator.process(Cursors.asTupleStreamPosition(cursor));
    }
    
    public void testPartial() throws Exception
    {
        cursor.advanceNextPosition();
        statsInlinedOperator.process(Cursors.asTupleStreamPosition(cursor));
        StatsInlinedOperator.Stats stats = StatsInlinedOperator.resultsAsStats(statsInlinedOperator.getResult());
        Assert.assertEquals(stats.getRowCount(), 2);
        Assert.assertEquals(stats.getRunsCount(), 1);
        Assert.assertEquals(stats.getMinPosition(), 0);
        Assert.assertEquals(stats.getMaxPosition(), 1);
        Assert.assertEquals(stats.getAvgRunLength(), 2);
        Assert.assertFalse(statsInlinedOperator.isFinished());
        Assert.assertEquals(statsInlinedOperator.getRange(), Range.create(0, 0));
    }
    
    public void testFull() throws Exception
    {
        cursor.advanceNextValue();
        statsInlinedOperator.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        statsInlinedOperator.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        StatsInlinedOperator.Stats stats = StatsInlinedOperator.resultsAsStats(statsInlinedOperator.getResult());
        Assert.assertEquals(stats.getRowCount(), 4);
        Assert.assertEquals(stats.getRunsCount(), 2);
        Assert.assertEquals(stats.getMinPosition(), 0);
        Assert.assertEquals(stats.getMaxPosition(), 3);
        Assert.assertEquals(stats.getAvgRunLength(), 2);
        Assert.assertFalse(statsInlinedOperator.isFinished());
        Assert.assertEquals(statsInlinedOperator.getRange(), Range.create(0, 0));
    }
}
