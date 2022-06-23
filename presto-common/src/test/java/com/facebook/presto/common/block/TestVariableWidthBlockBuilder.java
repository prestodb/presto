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
package com.facebook.presto.common.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.ceil;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    @Test
    public void testWriteBytes()
    {
        int entries = 100;
        String inputChars = "abcdefghijklmnopqrstuvwwxyz01234566789!@#$%^";
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, entries, entries);
        List<String> values = new ArrayList<>();
        Random rand = new Random(0);
        byte[] bytes = inputChars.getBytes(UTF_8);
        assertEquals(bytes.length, inputChars.length());
        for (int i = 0; i < entries; i++) {
            int valueLength = rand.nextInt(bytes.length);
            VARCHAR.writeBytes(blockBuilder, bytes, 0, valueLength);
            values.add(inputChars.substring(0, valueLength));
        }
        verifyBlockValues(blockBuilder, values);
        verifyBlockValues(blockBuilder.build(), values);
    }

    private void verifyBlockValues(Block block, List<String> values)
    {
        assertEquals(block.getPositionCount(), values.size());
        for (int i = 0; i < block.getPositionCount(); i++) {
            Slice slice = VARCHAR.getSlice(block, i);
            assertEquals(slice, utf8Slice(values.get(i)));
        }
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
