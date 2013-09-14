/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestDictionaryEncodedBlockCursorWithNulls
        extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(null, "apple", null, "banana", null, "banana", null, "banana", null, "cherry", null);
    }

    @Override
    protected BlockCursor createTestCursor()
    {
        Dictionary dictionary = new Dictionary(TupleInfo.SINGLE_VARBINARY,
                NULL_STRING_TUPLE.getTupleSlice(),
                createTuple("apple").getTupleSlice(),
                createTuple("banana").getTupleSlice(),
                createTuple("cherry").getTupleSlice(),
                createTuple("date").getTupleSlice());

        return new DictionaryEncodedBlock(dictionary, createLongsBlock(0, 1, 0, 2, 0, 2, 0, 2, 0, 3, 0)).cursor();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createTestCursor(), DictionaryEncodedBlockCursor.class);
    }
}
