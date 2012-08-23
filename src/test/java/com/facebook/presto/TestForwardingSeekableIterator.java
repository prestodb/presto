package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;


import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.RangePositionBlock.rangeGetter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestForwardingSeekableIterator
{
    private final static List<Range> RANGES = ImmutableList.of(
            Range.create(0L, 9L),
            Range.create(10L, 19L),
            Range.create(20L, 29L),
            Range.create(40L, 49L),
            Range.create(50L, 59L));

    @Test
    public void testBasicIteration()
    {
        ImmutableList<Range> actual = ImmutableList.copyOf(Iterators.transform(newIterator(), rangeGetter()));

        assertEquals(actual, RANGES);
    }

    @Test
    public void testPeek()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        assertEquals(iterator.peek().getRange(), Range.create(0L, 9L));
    }

    @Test
    public void testSeek()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        assertTrue(iterator.seekTo(25));
        assertTrue(iterator.hasNext());
        assertEquals(iterator.peek().getRange(), Range.create(20L, 29L));
    }

    @Test
    public void testSeekToCurrent()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        RangePositionBlock block = iterator.next();

        assertTrue(iterator.seekTo(3));
        assertTrue(iterator.hasNext());
        assertEquals(iterator.peek().getRange(), block.getRange());
        assertEquals(iterator.next().getRange(), block.getRange());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot seek to position.*")
    public void testSeekBackwards()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        RangePositionBlock first = iterator.next();
        iterator.next();

        iterator.seekTo(first.getRange().getStart());
    }

    @Test
    public void testSeekToMissing()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        assertFalse(iterator.seekTo(35));
        assertTrue(iterator.hasNext());
        assertEquals(iterator.peek().getRange(), Range.create(40L, 49L));
        assertEquals(iterator.next().getRange(), Range.create(40L, 49L));
    }

    @Test
    public void testSeekPastEnd()
    {
        ForwardingSeekableIterator<RangePositionBlock> iterator = newIterator();

        assertFalse(iterator.seekTo(80));
        assertFalse(iterator.hasNext());
    }

    private ForwardingSeekableIterator<RangePositionBlock> newIterator()
    {
        ImmutableList.Builder<RangePositionBlock> builder = ImmutableList.builder();

        for (Range range : RANGES) {
            builder.add(new RangePositionBlock(range));
        }

        return new ForwardingSeekableIterator<>(builder.build().iterator());
    }
}
