/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.Page;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.serde.PagesSerde.readPages;
import static com.facebook.presto.serde.PagesSerde.writePages;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    @Test
    public void testRoundTrip()
    {
        UncompressedBlock expectedBlock = new BlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();
        Page expectedPage = new Page(expectedBlock, expectedBlock, expectedBlock);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(sliceOutput, expectedPage, expectedPage, expectedPage);
        Iterator<Page> pageIterator = readPages(sliceOutput.slice().getInput());
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertFalse(pageIterator.hasNext());
    }
}
