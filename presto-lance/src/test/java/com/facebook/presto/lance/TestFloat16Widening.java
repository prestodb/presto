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
package com.facebook.presto.lance;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.common.block.Block;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestFloat16Widening
{
    private BufferAllocator allocator;
    private ArrowBlockBuilder arrowBlockBuilder;

    @BeforeMethod
    public void setUp()
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        arrowBlockBuilder = new ArrowBlockBuilder(createTestFunctionAndTypeManager());
    }

    @AfterMethod
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testWidenFloat2ToFloat4()
    {
        // Create a Float2Vector with test values
        try (Float2Vector f2v = new Float2Vector("f16_col", allocator)) {
            f2v.allocateNew(4);
            f2v.setWithPossibleTruncate(0, 1.5f);
            f2v.setWithPossibleTruncate(1, -2.25f);
            f2v.setNull(2);
            f2v.setWithPossibleTruncate(3, 0.0f);
            f2v.setValueCount(4);

            // Widen to Float4Vector
            try (Float4Vector f4v = widenFloat2ToFloat4(f2v)) {
                assertEquals(f4v.getValueCount(), 4);
                assertEquals(f4v.getName(), "f16_col");

                assertEquals(f4v.get(0), 1.5f, 0.01f);
                assertEquals(f4v.get(1), -2.25f, 0.01f);
                assertTrue(f4v.isNull(2));
                assertEquals(f4v.get(3), 0.0f, 0.01f);
            }
        }
    }

    @Test
    public void testWidenedFloat4VectorProducesRealBlock()
    {
        // Verify the widened Float4Vector works with ArrowBlockBuilder
        try (Float2Vector f2v = new Float2Vector("f16_col", allocator)) {
            f2v.allocateNew(3);
            f2v.setWithPossibleTruncate(0, 1.5f);
            f2v.setWithPossibleTruncate(1, -2.25f);
            f2v.setNull(2);
            f2v.setValueCount(3);

            try (Float4Vector f4v = widenFloat2ToFloat4(f2v)) {
                Block block = arrowBlockBuilder.buildBlockFromFieldVector(f4v, REAL, null);
                assertEquals(block.getPositionCount(), 3);

                float val0 = Float.intBitsToFloat((int) REAL.getLong(block, 0));
                float val1 = Float.intBitsToFloat((int) REAL.getLong(block, 1));
                assertEquals(val0, 1.5f, 0.01f);
                assertEquals(val1, -2.25f, 0.01f);
                assertTrue(block.isNull(2));
            }
        }
    }

    @Test
    public void testWidenEmptyVector()
    {
        try (Float2Vector f2v = new Float2Vector("empty", allocator)) {
            f2v.allocateNew(0);
            f2v.setValueCount(0);

            try (Float4Vector f4v = widenFloat2ToFloat4(f2v)) {
                assertEquals(f4v.getValueCount(), 0);
            }
        }
    }

    @Test
    public void testWidenAllNulls()
    {
        try (Float2Vector f2v = new Float2Vector("nulls", allocator)) {
            f2v.allocateNew(3);
            f2v.setNull(0);
            f2v.setNull(1);
            f2v.setNull(2);
            f2v.setValueCount(3);

            try (Float4Vector f4v = widenFloat2ToFloat4(f2v)) {
                assertEquals(f4v.getValueCount(), 3);
                assertTrue(f4v.isNull(0));
                assertTrue(f4v.isNull(1));
                assertTrue(f4v.isNull(2));
            }
        }
    }

    /**
     * Mirrors the private widenFloat2ToFloat4 method in LanceArrowToPageScanner.
     */
    private Float4Vector widenFloat2ToFloat4(Float2Vector f2v)
    {
        int valueCount = f2v.getValueCount();
        Float4Vector f4v = new Float4Vector(f2v.getName(), allocator);
        f4v.allocateNew(valueCount);
        for (int i = 0; i < valueCount; i++) {
            if (f2v.isNull(i)) {
                f4v.setNull(i);
            }
            else {
                f4v.set(i, f2v.getValueAsFloat(i));
            }
        }
        f4v.setValueCount(valueCount);
        return f4v;
    }
}
