package com.facebook.presto.aggregation;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.Blocks.createTupleStream;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static org.testng.Assert.assertEquals;

public class TestCountAggregation
{
    @Test
    public void testBasic()
            throws Exception
    {
        TupleStream values = createTupleStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(0, 3);

        assertCount(values, range, 4);
    }

    @Test
    public void testSubset()
            throws Exception
    {
        TupleStream values = createTupleStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(1, 2);

        assertCount(values, range, 2);
    }

    @Test
    public void testNonOverlapping()
            throws Exception
    {
        TupleStream values = createTupleStream(0, "apple", "banana", "cherry", "date");
        Range range = Range.create(10, 20);

        assertCount(values, range, 0);
    }

    @Test
    public void testSparse()
            throws Exception
    {
        TupleStream values = createTupleStream(0, "apple", "banana", "cherry", "date");
        List<Range> ranges = ImmutableList.of(Range.create(1, 1), Range.create(3, 3));

        assertCount(values, ranges, 2);
    }

    private void assertCount(TupleStream values, Range range, int expected)
    {
        assertCount(values, ImmutableList.of(range), expected);
    }

    private void assertCount(TupleStream values, Iterable<Range> ranges, int expected)
    {
        Cursor cursor = values.cursor(new QuerySession());
        assertAdvanceNextPosition(cursor);

        CountAggregation count = new CountAggregation();
        for (Range range : ranges) {
            if (Cursors.advanceToPositionNoYield(cursor, range.getStart())) {
                count.add(cursor, range.getEnd());
            }
        }

        assertEquals(count.evaluate().getLong(0), expected);
    }
}
