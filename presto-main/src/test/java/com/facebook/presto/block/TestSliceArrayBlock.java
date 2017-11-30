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

import com.facebook.presto.spi.block.SliceArrayBlock;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

public class TestSliceArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertVariableWithValues(expectedValues, false);
        assertVariableWithValues((Slice[]) alternatingNullValues(expectedValues), false);
    }

    @Test
    public void testNonDistinctSlices()
    {
        int positionCount = 5;
        Slice[] baseSlices = createExpectedUniqueValues(positionCount);
        Slice[] baseSlicesCopy = createExpectedUniqueValues(positionCount);
        Slice[] testSlices = new Slice[positionCount * 3];
        // the first and second five slices point to baseSlices, and the third five slices point to baseSlicesCopy
        for (int i = 0; i < positionCount; i++) {
            testSlices[i] = baseSlices[i];
            testSlices[positionCount + i] = baseSlices[i];
            testSlices[positionCount * 2 + i] = baseSlicesCopy[i];
        }

        SliceArrayBlock block = new SliceArrayBlock(testSlices.length, testSlices, false);
        assertBlock(block, testSlices);
    }

    @Test
    public void testDistinctSlices()
    {
        Slice[] expectedValues = createExpectedUniqueValues(100);
        assertVariableWithValues(expectedValues, true);
    }

    @Test
    public void testCopyPositions()
            throws Exception
    {
        Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(100));
        SliceArrayBlock block = new SliceArrayBlock(expectedValues.length, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, Ints.asList(0, 2, 4, 6, 7, 9, 10, 16));
    }

    private void assertVariableWithValues(Slice[] expectedValues, boolean valueSlicesAreDistinct)
    {
        SliceArrayBlock block = new SliceArrayBlock(expectedValues.length, expectedValues, valueSlicesAreDistinct);
        assertBlock(block, expectedValues);
    }
}
