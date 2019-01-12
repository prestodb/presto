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
package io.prestosql.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.ceil;
import static org.openjdk.jol.info.ClassLayout.parseClass;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVariableWidthBlockBuilder
{
    private static final int BLOCK_BUILDER_INSTANCE_SIZE = parseClass(VariableWidthBlockBuilder.class).instanceSize();
    private static final int SLICE_INSTANCE_SIZE = parseClass(DynamicSliceOutput.class).instanceSize() + parseClass(Slice.class).instanceSize();
    private static final int VARCHAR_VALUE_SIZE = 7;
    private static final int VARCHAR_ENTRY_SIZE = SIZE_OF_INT + VARCHAR_VALUE_SIZE;
    private static final int EXPECTED_ENTRY_COUNT = 3;

    @Test
    public void testFixedBlockIsFull()
    {
        testIsFull(new PageBuilderStatus(VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT));
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        int entries = 12345;
        double resetSkew = 1.25;
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, entries, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeByte(i);
            blockBuilder.closeEntry();
        }
        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        // force to initialize capacity
        blockBuilder.writeByte(1);

        long actualArrayBytes = sizeOf(new int[(int) ceil(resetSkew * (entries + 1))]) + sizeOf(new boolean[(int) ceil(resetSkew * entries)]);
        long actualSliceBytes = SLICE_INSTANCE_SIZE + sizeOf(new byte[(int) ceil(resetSkew * entries)]);
        assertEquals(blockBuilder.getRetainedSizeInBytes(), BLOCK_BUILDER_INSTANCE_SIZE + actualSliceBytes + actualArrayBytes);
    }

    private void testIsFull(PageBuilderStatus pageBuilderStatus)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 32, 1024);
        assertTrue(pageBuilderStatus.isEmpty());
        while (!pageBuilderStatus.isFull()) {
            VARCHAR.writeSlice(blockBuilder, Slices.allocate(VARCHAR_VALUE_SIZE));
        }
        assertEquals(blockBuilder.getPositionCount(), EXPECTED_ENTRY_COUNT);
        assertEquals(pageBuilderStatus.isFull(), true);
    }
}
