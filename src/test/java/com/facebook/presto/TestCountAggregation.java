package com.facebook.presto;

import com.facebook.presto.aggregations.CountAggregation;
import org.testng.annotations.Test;

import static com.facebook.presto.Blocks.createBlock;
import static org.testng.Assert.assertEquals;

public class TestCountAggregation
{
    @Test
    public void testBasic()
            throws Exception
    {
        ValueBlock values = createBlock(0, "apple", "banana", "cherry", "date");
        PositionBlock positions = new RangePositionBlock(Range.create(0, 3));

        assertCount(values, positions, 4);
    }

    @Test
    public void testSubset()
            throws Exception
    {
        ValueBlock values = createBlock(0, "apple", "banana", "cherry", "date");
        PositionBlock positions = new RangePositionBlock(Range.create(1, 2));

        assertCount(values, positions, 2);
    }

    @Test
    public void testNonOverlapping()
            throws Exception
    {
        ValueBlock values = createBlock(0, "apple", "banana", "cherry", "date");
        PositionBlock positions = new RangePositionBlock(Range.create(10, 20));

        assertCount(values, positions, 0);
    }

    @Test
    public void testSparse()
            throws Exception
    {
        ValueBlock values = createBlock(0, "apple", "banana", "cherry", "date");
        PositionBlock positions = new UncompressedPositionBlock(1, 3);

        assertCount(values, positions, 2);
    }

    private void assertCount(ValueBlock values, PositionBlock positions, int expected)
    {
        CountAggregation count = new CountAggregation();
        count.add(values, positions);

        assertEquals(count.evaluate().getLong(0), expected);
    }
}
