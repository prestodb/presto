package com.facebook.presto;

import com.facebook.presto.aggregations.CountAggregation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.Blocks.createBlockStream;
import static org.testng.Assert.assertEquals;

public class TestCountAggregation
{
    @Test
    public void testBasic()
            throws Exception
    {
        UncompressedBlockStream values = createBlockStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(0, 3);

        assertCount(values, range, 4);
    }

    @Test
    public void testSubset()
            throws Exception
    {
        UncompressedBlockStream values = createBlockStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(1, 2);

        assertCount(values, range, 2);
    }

    @Test
    public void testNonOverlapping()
            throws Exception
    {
        UncompressedBlockStream values = createBlockStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(10, 20);

        assertCount(values, range, 0);
    }

    @Test
    public void testSparse()
            throws Exception
    {
        UncompressedBlockStream values = createBlockStream(0, "apple", "banana", "cherry", "date");
        List<Range> ranges = ImmutableList.of(Range.create(1, 1), Range.create(3, 3));

        assertCount(values, ranges, 2);
    }

    private void assertCount(UncompressedBlockStream values, Range range, int expected)
    {
        assertCount(values, ImmutableList.of(range), expected);
    }

    private void assertCount(UncompressedBlockStream values, Iterable<Range> ranges, int expected)
    {
        Cursor cursor = values.cursor();
        cursor.advanceNextPosition();

        CountAggregation count = new CountAggregation();
        for (Range range : ranges) {
            count.add(cursor, range);
        }

        assertEquals(count.evaluate().getLong(0), expected);
    }
}
