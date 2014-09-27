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

import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

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
        TestLazySliceArrayBlockLoader loader = new TestLazySliceArrayBlockLoader(new Slice[10]);
        LazySliceArrayBlock block = new LazySliceArrayBlock(10, loader);

        block.assureLoaded();

        // verify load was called
        assertTrue(loader.loaded);
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
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final Slice[] expectedValues;
        private boolean loaded;

        public TestLazySliceArrayBlockLoader(Slice[] expectedValues)
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
}
