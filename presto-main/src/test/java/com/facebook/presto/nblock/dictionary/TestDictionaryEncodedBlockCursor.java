/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock.dictionary;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.AbstractTestBlockCursor;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.Tuples.createTuple;
import static com.facebook.presto.nblock.BlockAssertions.createLongsBlock;
import static com.facebook.presto.nblock.BlockAssertions.createStringsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestDictionaryEncodedBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Override
    protected BlockCursor createTestCursor()
    {
        Dictionary dictionary = new Dictionary(TupleInfo.SINGLE_VARBINARY,
                createTuple("apple").getTupleSlice(),
                createTuple("banana").getTupleSlice(),
                createTuple("cherry").getTupleSlice(),
                createTuple("date").getTupleSlice());

        return new DictionaryEncodedBlock(dictionary, createLongsBlock(0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3)).cursor();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createTestCursor(), DictionaryEncodedBlockCursor.class);
    }
}
