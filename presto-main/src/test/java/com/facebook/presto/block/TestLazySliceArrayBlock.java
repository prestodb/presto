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

import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock.LazySliceArrayBlockLoader;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkState;
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
        TestLazySliceArrayBlockLoader loader = new TestLazySliceArrayBlockLoader(null);
        LazySliceArrayBlock block = new LazySliceArrayBlock(10, loader);

        // release the block
        block.release();

        // verify release was called
        assertTrue(loader.released);

        // verify methods accessing the data throw IllegalStateException
        try {
            block.isNull(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getLength(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getByte(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getInt(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getLong(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getDouble(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            block.getSlice(0, 0, 1);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    private static void assertVariableWithValues(Slice[] expectedValues)
    {
        LazySliceArrayBlock block = new LazySliceArrayBlock(expectedValues.length, new TestLazySliceArrayBlockLoader(expectedValues));
        assertBlock(block, expectedValues);
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static class TestLazySliceArrayBlockLoader
            implements LazySliceArrayBlockLoader
    {
        private final Slice[] expectedValues;
        private boolean released;

        public TestLazySliceArrayBlockLoader(Slice[] expectedValues)
        {
            this.expectedValues = expectedValues;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            checkState(!released, "Block has been released");

            if (expectedValues == null) {
                fail("load should not be called");
            }

            block.setValues(expectedValues);
        }

        @Override
        public void release()
        {
            released = true;
        }
    }
}
