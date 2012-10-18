package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRange
{
    @Test
    public void testBasic()
    {
        Range range = Range.create(10, 20);
        assertEquals(range.getStart(), 10);
        assertEquals(range.getEnd(), 20);
    }

    @Test
    public void testSize()
    {
        assertEquals(Range.create(0, 0).length(), 1);
        assertEquals(Range.create(0, 9).length(), 10);
        assertEquals(Range.create(9, 18).length(), 10);
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(Range.create(0, 0).overlaps(Range.create(0, 0)));
        assertTrue(Range.create(0, 10).overlaps(Range.create(10, 20)));
        assertTrue(Range.create(0, 10).overlaps(Range.create(5, 20)));
        assertTrue(Range.create(0, 10).overlaps(Range.create(0, 10)));
        assertFalse(Range.create(0, 10).overlaps(Range.create(20, 30)));
        assertFalse(Range.create(20, 30).overlaps(Range.create(0, 10)));
    }

    @Test
    public void testContainsValue()
    {
        assertTrue(Range.create(0, 0).contains(0));
        assertFalse(Range.create(0, 0).contains(-1));
        assertFalse(Range.create(0, 0).contains(1));

        assertFalse(Range.create(0, 10).contains(-1));
        assertTrue(Range.create(0, 10).contains(0));
        assertTrue(Range.create(0, 10).contains(5));
        assertTrue(Range.create(0, 10).contains(10));
        assertFalse(Range.create(0, 10).contains(11));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(Range.create(0, 20).intersect(Range.create(5, 15)), Range.create(5, 15));
        assertEquals(Range.create(5, 20).intersect(Range.create(0, 10)), Range.create(5, 10));
        assertEquals(Range.create(0, 20).intersect(Range.create(15, 30)), Range.create(15, 20));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*<=.*")
    public void createInvalid()
    {
        Range.create(10, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*do not overlap.*")
    public void testIntersectNonOverlapping()
    {
        Range.create(0, 10).intersect(Range.create(20, 30));
    }

    @Test
    public void testIteration()
    {
        assertEquals(ImmutableList.copyOf(Range.create(0, 0)), ImmutableList.of(0L));
        assertEquals(ImmutableList.copyOf(Range.create(0, 5)), ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L));
    }
}
