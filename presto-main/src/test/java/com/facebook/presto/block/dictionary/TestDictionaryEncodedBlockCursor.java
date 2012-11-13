/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.dictionary;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestDictionaryEncodedBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock("apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Override
    protected BlockCursor createTestCursor()
    {
        Dictionary dictionary = new Dictionary(TupleInfo.SINGLE_VARBINARY,
                createTuple("apple").getTupleSlice(),
                createTuple("banana").getTupleSlice(),
                createTuple("cherry").getTupleSlice(),
                createTuple("date").getTupleSlice());

        return new DictionaryEncodedBlock(dictionary, createLongsBlock(0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3)).cursor();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createTestCursor(), DictionaryEncodedBlockCursor.class);
    }
}
