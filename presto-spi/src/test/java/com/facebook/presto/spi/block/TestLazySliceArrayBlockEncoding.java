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
package com.facebook.presto.spi.block;

import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLazySliceArrayBlockEncoding
{
    @Test
    public void testDirectLazySliceArrayBlockEncoding()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        TestDirectLazySliceArrayBlockLoader loader = new TestDirectLazySliceArrayBlockLoader(expectedValues);
        LazySliceArrayBlock lazySliceArrayBlock = new LazySliceArrayBlock(10, loader);

        lazySliceArrayBlock.assureLoaded();
        assertTrue(loader.loaded);

        BlockEncoding blockEncoding = new LazySliceArrayBlockEncoding();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncoding.writeBlock(sliceOutput, lazySliceArrayBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());

        assertTrue(actualBlock instanceof SliceArrayBlock);
        SliceArrayBlock sliceArrayBlock = (SliceArrayBlock) actualBlock;
        assertEquals(sliceArrayBlock.getValues(), expectedValues);
    }

    @Test
    public void testDirectLazySliceArrayBlockEncodingWithNulls()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValuesWithNulls(10, Ints.asList(0, 3, 9));
        TestDirectLazySliceArrayBlockLoader loader = new TestDirectLazySliceArrayBlockLoader(expectedValues);
        LazySliceArrayBlock lazySliceArrayBlock = new LazySliceArrayBlock(10, loader);

        lazySliceArrayBlock.assureLoaded();
        assertTrue(loader.loaded);

        BlockEncoding blockEncoding = new LazySliceArrayBlockEncoding();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncoding.writeBlock(sliceOutput, lazySliceArrayBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());

        assertTrue(actualBlock instanceof SliceArrayBlock);
        SliceArrayBlock sliceArrayBlock = (SliceArrayBlock) actualBlock;
        assertEquals(sliceArrayBlock.getValues(), expectedValues);
    }

    @Test
    public void testDictionaryLazySliceArrayBlockEncodingWithNullVector()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(7);
        int[] ids = new int[] { 0, 2, 1, 0, 0, 0, 1, 1, 1, 0, 1, 2 };
        boolean[] isNull = new boolean[ids.length];
        isNull[3] = true;

        TestDictionaryLazySliceArrayBlockLoader loader = new TestDictionaryLazySliceArrayBlockLoader(expectedValues, ids, isNull);
        LazySliceArrayBlock lazySliceArrayBlock = new LazySliceArrayBlock(ids.length, loader);

        lazySliceArrayBlock.assureLoaded();
        assertTrue(loader.loaded);

        BlockEncoding blockEncoding = new LazySliceArrayBlockEncoding();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);

        blockEncoding.writeBlock(sliceOutput, lazySliceArrayBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());

        assertTrue(actualBlock instanceof DictionaryBlock);
        DictionaryBlock dictionaryBlock = (DictionaryBlock) actualBlock;

        Block dictionary = dictionaryBlock.getDictionary();
        assertTrue(dictionary instanceof SliceArrayBlock);
        SliceArrayBlock dictionarySliceArrayBlock = (SliceArrayBlock) dictionary;

        // check that we compacted the dictionary
        Slice[] expectedSlices = Arrays.copyOfRange(expectedValues, 0, 3);

        // add 1 for storing null
        assertEquals(dictionarySliceArrayBlock.getValues(), Arrays.copyOf(expectedSlices, expectedSlices.length + 1));

        // change the mapping for the null value
        ids[3] = expectedSlices.length;
        assertEquals(dictionaryBlock.getIds(), Slices.wrappedIntArray(ids));
        assertTrue(dictionaryBlock.isNull(3));
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice[] createExpectedValuesWithNulls(int positionCount, List<Integer> nullIndexes)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (nullIndexes.contains(position)) {
                continue;
            }
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    private static class TestDirectLazySliceArrayBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final Slice[] expectedValues;
        private boolean loaded;

        public TestDirectLazySliceArrayBlockLoader(Slice[] expectedValues)
        {
            this.expectedValues = expectedValues;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            if (expectedValues == null) {
                fail("load should not be called");
            }

            block.setValues(expectedValues);
            loaded = true;
        }
    }

    private static class TestDictionaryLazySliceArrayBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final Slice[] values;
        private final int[] ids;
        private final boolean[] isNull;
        private boolean loaded;

        public TestDictionaryLazySliceArrayBlockLoader(Slice[] values, int[] ids, boolean[] isNull)
        {
            this.values = values;
            this.ids = ids;
            this.isNull = isNull;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            if (values == null) {
                fail("load should not be called");
            }

            block.setValues(values, ids, isNull);
            loaded = true;
        }
    }
}
