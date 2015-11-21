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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.block.SliceArrayBlock.getSliceArraySizeInBytes;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLazySliceArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertVariableWithValues(expectedValues);
        assertVariableWithValues((Slice[]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testRelease()
    {
        TestDirectLazySliceArrayBlockLoader loader = new TestDirectLazySliceArrayBlockLoader(new Slice[10]);
        LazySliceArrayBlock block = new LazySliceArrayBlock(10, loader);

        block.assureLoaded();

        // verify load was called
        assertTrue(loader.loaded);
    }

    @Test
    public void testDirectBlock()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        TestDirectLazySliceArrayBlockLoader loader = new TestDirectLazySliceArrayBlockLoader(expectedValues);
        LazySliceArrayBlock block = new LazySliceArrayBlock(10, loader);

        block.assureLoaded();
        assertEquals(block.getValues(), expectedValues);
    }

    @Test
    public void testDictionaryBlock()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(3);
        int[] ids = new int[] { 0, 2, 1, 0, 0, 0, 1, 1, 1, 0, 1, 2 };
        boolean[] isNull = new boolean[ids.length];

        TestDictionaryLazySliceArrayBlockLoader loader = new TestDictionaryLazySliceArrayBlockLoader(expectedValues, ids, isNull);
        LazySliceArrayBlock block = new LazySliceArrayBlock(ids.length, loader);

        block.assureLoaded();

        assertTrue(block.isDictionary());
        assertEquals(block.getValues(), expectedValues);
        assertEquals(block.getIds(), ids);

        int expectedSizeInBytes = getSliceArraySizeInBytes(expectedValues) + (ids.length * SIZE_OF_INT) + (isNull.length * SIZE_OF_BYTE);
        assertEquals(block.getSizeInBytes(), expectedSizeInBytes);
    }

    @Test
    public void testDictionaryBlockGetRegion()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(3);
        int[] ids = new int[] { 0, 2, 1, 0, 0, 0, 1, 1, 1, 0, 1, 2 };
        boolean[] isNull = new boolean[ids.length];
        isNull[2] = true;

        LazyBlockLoader<LazySliceArrayBlock> loader = new TestDictionaryLazySliceArrayBlockLoader(expectedValues, ids, isNull);
        LazySliceArrayBlock block = new LazySliceArrayBlock(ids.length, loader);

        Block region = block.getRegion(0, 3);
        assertFalse(region.isNull(0));
        assertFalse(region.isNull(1));
        assertTrue(region.isNull(2));

        assertTrue(region instanceof DictionaryBlock);
        DictionaryBlock dictionaryBlock = (DictionaryBlock) region;
        assertEquals(((SliceArrayBlock) dictionaryBlock.getDictionary()).getValues(), new Slice[] { expectedValues[0], expectedValues[2], null });
        // The values in the dictionary are rearranged during compaction in the order in which they are referenced,
        // with a null appended to the end of the list if applicable
        assertEquals(dictionaryBlock.getIds(), Slices.wrappedIntArray(0, 1, 2));
    }

    private void assertVariableWithValues(Slice[] expectedValues)
    {
        LazySliceArrayBlock block = new LazySliceArrayBlock(expectedValues.length, new TestDirectLazySliceArrayBlockLoader(expectedValues));
        assertBlock(block, expectedValues);
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
