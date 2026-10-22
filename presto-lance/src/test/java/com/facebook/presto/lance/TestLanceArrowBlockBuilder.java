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
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceArrowBlockBuilder
{
    private BufferAllocator allocator;
    private ArrowBlockBuilder blockBuilder;

    @BeforeMethod
    public void setUp()
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        blockBuilder = new ArrowBlockBuilder(createTestFunctionAndTypeManager());
    }

    @AfterMethod
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testFloat16WidensToReal()
    {
        try (Float2Vector vector = new Float2Vector("f16", allocator)) {
            vector.allocateNew(3);
            vector.setSafe(0, Float16.toFloat16(3.5f));
            vector.setSafe(1, Float16.toFloat16(-3.5f));
            vector.setNull(2);
            vector.setValueCount(3);
            Block block = blockBuilder.buildBlockFromFieldVector(vector, REAL, null);
            assertEquals(Float.intBitsToFloat((int) REAL.getLong(block, 0)), 3.5f);
            assertEquals(Float.intBitsToFloat((int) REAL.getLong(block, 1)), -3.5f);
            assertTrue(block.isNull(2));
        }
    }

    @Test
    public void testUnsignedInt32ReadsAsBigint()
    {
        try (UInt4Vector vector = new UInt4Vector("u32", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, -1); // 0xFFFFFFFF == 4294967295 when unsigned
            vector.setSafe(1, 7);
            vector.setValueCount(2);
            Block block = blockBuilder.buildBlockFromFieldVector(vector, BIGINT, null);
            assertEquals(BIGINT.getLong(block, 0), 4294967295L);
            assertEquals(BIGINT.getLong(block, 1), 7L);
        }
    }

    @Test
    public void testUnsignedInt64ReadsAsBigint()
    {
        try (UInt8Vector vector = new UInt8Vector("u64", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 42L);
            vector.setValueCount(1);
            Block block = blockBuilder.buildBlockFromFieldVector(vector, BIGINT, null);
            assertEquals(BIGINT.getLong(block, 0), 42L);
        }
    }
}
