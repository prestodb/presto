/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaskedValueBlock
{
    @Test
    public void testMaskUncompressedBlock()
            throws Exception
    {
        ValueBlock valueBlock = createBlock(10, "alice", "bob", "charlie", "david", "eric", "frank", "greg", "hank", "ian", "jenny");
        Optional<ValueBlock> masked = MaskedValueBlock.maskBlock(valueBlock, new UncompressedPositionBlock(8, 10, 12, 14, 16, 100));

        assertTrue(masked.isPresent());
        ValueBlock tuples = masked.get();

        assertEquals(tuples.getCount(), 4);
        assertEquals(ImmutableList.copyOf(tuples.getPositions()), ImmutableList.of(10L, 12L, 14L, 16L));
        assertEquals(tuples.getRange(), Range.create(10L, 16L));

        assertFalse(tuples.isPositionsContiguous());
        assertFalse(tuples.isSingleValue());
        assertFalse(tuples.isSorted());

        assertEquals(ImmutableList.copyOf(tuples.iterator()), ImmutableList.of(createTuple("alice"), createTuple("charlie"), createTuple("eric"), createTuple("greg")));

        assertEquals(ImmutableList.copyOf(tuples.pairIterator()), ImmutableList.of(
                new Pair(10, createTuple("alice")),
                new Pair(12, createTuple("charlie")),
                new Pair(14, createTuple("eric")),
                new Pair(16, createTuple("greg"))));

        assertEquals(ImmutableList.copyOf(tuples.filter(new UncompressedPositionBlock(12, 16)).get().pairIterator()), ImmutableList.of(
                new Pair(12, createTuple("charlie")),
                new Pair(16, createTuple("greg"))));
    }

    @Test
    public void testMaskRunLengthEncodedBlock()
            throws Exception
    {
        ValueBlock valueBlock = new RunLengthEncodedBlock(createTuple("run"), Range.create(10L, 19L));
        Optional<ValueBlock> masked = MaskedValueBlock.maskBlock(valueBlock, new RangePositionBlock(Range.create(12L, 15L)));

        assertTrue(masked.isPresent());

        ValueBlock tuples = masked.get();
        assertEquals(tuples.getCount(), 4);
        assertEquals(ImmutableList.copyOf(tuples.getPositions()), ImmutableList.of(12L, 13L, 14L, 15L));
        assertEquals(tuples.getRange(), Range.create(12L, 15L));

        assertTrue(tuples.isPositionsContiguous());
        assertTrue(tuples.isSingleValue());
        assertTrue(tuples.isSorted());

        assertEquals(ImmutableList.copyOf(tuples.iterator()), ImmutableList.of(createTuple("run"), createTuple("run"), createTuple("run"), createTuple("run")));

        assertEquals(ImmutableList.copyOf(tuples.pairIterator()), ImmutableList.of(
                new Pair(12, createTuple("run")),
                new Pair(13, createTuple("run")),
                new Pair(14, createTuple("run")),
                new Pair(15, createTuple("run"))));

        ValueBlock secondFilter = tuples.filter(new UncompressedPositionBlock(13, 15)).get();
        assertFalse(secondFilter.isPositionsContiguous());
        assertTrue(secondFilter.isSingleValue());
        assertTrue(secondFilter.isSorted());
        assertEquals(ImmutableList.copyOf(secondFilter.pairIterator()), ImmutableList.of(
                new Pair(13, createTuple("run")),
                new Pair(15, createTuple("run"))));
    }

    private Tuple createTuple(String value)
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY);
        return tupleInfo.builder().append(Slices.copiedBuffer(value, UTF_8)).build();
    }
}
