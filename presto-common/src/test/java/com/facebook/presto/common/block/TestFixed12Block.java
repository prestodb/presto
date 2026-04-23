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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFixed12Block
{
    @Test
    public void testWriteAndRead()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 10);
        builder.writeFixed12(100L, 42);
        builder.writeFixed12(200L, 84);
        builder.writeFixed12(-1L, 999999);
        builder.writeFixed12(Long.MAX_VALUE, 0);
        builder.writeFixed12(Long.MIN_VALUE, 500000);

        Fixed12Block block = (Fixed12Block) builder.build();

        assertEquals(block.getPositionCount(), 5);
        assertEquals(block.getFixed12First(0), 100L);
        assertEquals(block.getFixed12Second(0), 42);
        assertEquals(block.getFixed12First(1), 200L);
        assertEquals(block.getFixed12Second(1), 84);
        assertEquals(block.getFixed12First(2), -1L);
        assertEquals(block.getFixed12Second(2), 999999);
        assertEquals(block.getFixed12First(3), Long.MAX_VALUE);
        assertEquals(block.getFixed12Second(3), 0);
        assertEquals(block.getFixed12First(4), Long.MIN_VALUE);
        assertEquals(block.getFixed12Second(4), 500000);
    }

    @Test
    public void testNullHandling()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(100L, 1);
        builder.appendNull();
        builder.writeFixed12(300L, 3);

        Block block = builder.build();

        assertEquals(block.getPositionCount(), 3);
        assertFalse(block.isNull(0));
        assertTrue(block.isNull(1));
        assertFalse(block.isNull(2));
    }

    @Test
    public void testGetRegion()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(10L, 1);
        builder.writeFixed12(20L, 2);
        builder.writeFixed12(30L, 3);
        builder.writeFixed12(40L, 4);

        Fixed12Block block = (Fixed12Block) builder.build();
        Block region = block.getRegion(1, 2);

        assertEquals(region.getPositionCount(), 2);
        assertEquals(((Fixed12Block) region).getFixed12First(0), 20L);
        assertEquals(((Fixed12Block) region).getFixed12Second(0), 2);
        assertEquals(((Fixed12Block) region).getFixed12First(1), 30L);
        assertEquals(((Fixed12Block) region).getFixed12Second(1), 3);
    }

    @Test
    public void testCopyPositions()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(10L, 1);
        builder.writeFixed12(20L, 2);
        builder.writeFixed12(30L, 3);
        builder.writeFixed12(40L, 4);

        Fixed12Block block = (Fixed12Block) builder.build();
        Block copied = block.copyPositions(new int[] {0, 2, 3}, 0, 3);

        assertEquals(copied.getPositionCount(), 3);
        assertEquals(((Fixed12Block) copied).getFixed12First(0), 10L);
        assertEquals(((Fixed12Block) copied).getFixed12Second(0), 1);
        assertEquals(((Fixed12Block) copied).getFixed12First(1), 30L);
        assertEquals(((Fixed12Block) copied).getFixed12Second(1), 3);
        assertEquals(((Fixed12Block) copied).getFixed12First(2), 40L);
        assertEquals(((Fixed12Block) copied).getFixed12Second(2), 4);
    }

    @Test
    public void testGetSingleValueBlock()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(100L, 42);
        builder.writeFixed12(200L, 84);

        Fixed12Block block = (Fixed12Block) builder.build();
        Block singleValueBlock = block.getSingleValueBlock(1);

        assertEquals(singleValueBlock.getPositionCount(), 1);
        assertEquals(((Fixed12Block) singleValueBlock).getFixed12First(0), 200L);
        assertEquals(((Fixed12Block) singleValueBlock).getFixed12Second(0), 84);
    }

    @Test
    public void testCopyRegion()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(10L, 1);
        builder.writeFixed12(20L, 2);
        builder.writeFixed12(30L, 3);
        builder.writeFixed12(40L, 4);

        Fixed12Block block = (Fixed12Block) builder.build();
        Block copy = block.copyRegion(1, 2);

        assertEquals(copy.getPositionCount(), 2);
        assertEquals(((Fixed12Block) copy).getFixed12First(0), 20L);
        assertEquals(((Fixed12Block) copy).getFixed12Second(0), 2);
        assertEquals(((Fixed12Block) copy).getFixed12First(1), 30L);
        assertEquals(((Fixed12Block) copy).getFixed12Second(1), 3);
    }

    @Test
    public void testSizeInBytes()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(10L, 1);
        builder.writeFixed12(20L, 2);
        builder.writeFixed12(30L, 3);

        Block block = builder.build();
        assertEquals(block.getSizeInBytes(), Fixed12Block.SIZE_IN_BYTES_PER_POSITION * 3L);
    }

    @Test
    public void testEncodeFixed12()
    {
        int[] target = new int[3];
        Fixed12Block.encodeFixed12(0x0000000100000002L, 42, target, 0);
        // Verify the encoding: low 32 bits, high 32 bits, int value
        assertEquals(target[0], 0x00000002); // low 32 bits
        assertEquals(target[1], 0x00000001); // high 32 bits
        assertEquals(target[2], 42);
    }

    @Test
    public void testEncodeFixed12Roundtrip()
    {
        long[] testFirstValues = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 1234567890123456L};
        int[] testSecondValues = {0, 1, 999999, 500000, Integer.MAX_VALUE};

        for (long first : testFirstValues) {
            for (int second : testSecondValues) {
                int[] target = new int[3];
                Fixed12Block.encodeFixed12(first, second, target, 0);

                // Read back using the same logic as Fixed12Block
                long readFirst = (target[0] & 0xFFFFFFFFL) | ((long) target[1] << 32);
                int readSecond = target[2];

                assertEquals(readFirst, first, "Roundtrip failed for first=" + first);
                assertEquals(readSecond, second, "Roundtrip failed for second=" + second);
            }
        }
    }

    @Test
    public void testWritePositionTo()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(100L, 42);
        builder.writeFixed12(200L, 84);

        Fixed12Block block = (Fixed12Block) builder.build();

        Fixed12BlockBuilder targetBuilder = new Fixed12BlockBuilder(null, 5);
        block.writePositionTo(0, targetBuilder);
        block.writePositionTo(1, targetBuilder);

        Fixed12Block targetBlock = (Fixed12Block) targetBuilder.build();
        assertEquals(targetBlock.getPositionCount(), 2);
        assertEquals(targetBlock.getFixed12First(0), 100L);
        assertEquals(targetBlock.getFixed12Second(0), 42);
        assertEquals(targetBlock.getFixed12First(1), 200L);
        assertEquals(targetBlock.getFixed12Second(1), 84);
    }

    @Test
    public void testAppendNull()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(100L, 42);

        Fixed12Block block = (Fixed12Block) builder.build();
        Block withNull = block.appendNull();

        assertEquals(withNull.getPositionCount(), 2);
        assertFalse(withNull.isNull(0));
        assertTrue(withNull.isNull(1));
    }

    @Test
    public void testEmptyBlock()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 0);
        Block block = builder.build();
        assertEquals(block.getPositionCount(), 0);
    }

    @Test
    public void testAllNulls()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 3);
        builder.appendNull();
        builder.appendNull();
        builder.appendNull();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 3);
        assertTrue(block.isNull(0));
        assertTrue(block.isNull(1));
        assertTrue(block.isNull(2));
    }

    @Test
    public void testBuilderNewBlockBuilderLike()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 10);
        builder.writeFixed12(100L, 42);
        builder.writeFixed12(200L, 84);

        BlockBuilder newBuilder = builder.newBlockBuilderLike(null);
        assertTrue(newBuilder instanceof Fixed12BlockBuilder);
    }

    @Test
    public void testConstructorWithOptionalNull()
    {
        int[] values = new int[6]; // 2 entries * 3 ints per entry
        Fixed12Block.encodeFixed12(100L, 42, values, 0);
        Fixed12Block.encodeFixed12(200L, 84, values, 1);

        Fixed12Block block = new Fixed12Block(2, Optional.empty(), values);
        assertEquals(block.getPositionCount(), 2);
        assertFalse(block.mayHaveNull());
        assertEquals(block.getFixed12First(0), 100L);
        assertEquals(block.getFixed12Second(0), 42);
        assertEquals(block.getFixed12First(1), 200L);
        assertEquals(block.getFixed12Second(1), 84);
    }

    @Test
    public void testConstructorWithNulls()
    {
        int[] values = new int[6]; // 2 entries
        Fixed12Block.encodeFixed12(100L, 42, values, 0);
        boolean[] nulls = new boolean[] {false, true};

        Fixed12Block block = new Fixed12Block(2, Optional.of(nulls), values);
        assertEquals(block.getPositionCount(), 2);
        assertTrue(block.mayHaveNull());
        assertFalse(block.isNull(0));
        assertTrue(block.isNull(1));
    }

    @Test
    public void testLargeValues()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 3);
        builder.writeFixed12(Long.MAX_VALUE, 999999);
        builder.writeFixed12(Long.MIN_VALUE, 0);
        builder.writeFixed12(0L, 500000);

        Fixed12Block block = (Fixed12Block) builder.build();
        assertEquals(block.getFixed12First(0), Long.MAX_VALUE);
        assertEquals(block.getFixed12Second(0), 999999);
        assertEquals(block.getFixed12First(1), Long.MIN_VALUE);
        assertEquals(block.getFixed12Second(1), 0);
        assertEquals(block.getFixed12First(2), 0L);
        assertEquals(block.getFixed12Second(2), 500000);
    }

    @Test
    public void testGetLong()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 2);
        builder.writeFixed12(12345L, 42);

        Fixed12Block block = (Fixed12Block) builder.build();
        // getLong(position, 0) should return the first long value
        assertEquals(block.getLong(0, 0), 12345L);
    }

    @Test
    public void testGetInt()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 2);
        builder.writeFixed12(12345L, 42);

        Fixed12Block block = (Fixed12Block) builder.build();
        // getInt(position) should return the second int value
        assertEquals(block.getInt(0), 42);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetLongInvalidOffset()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 2);
        builder.writeFixed12(12345L, 42);
        Fixed12Block block = (Fixed12Block) builder.build();
        block.getLong(0, 1); // Should throw since only offset 0 is valid
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testReadOutOfBounds()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 2);
        builder.writeFixed12(12345L, 42);
        Fixed12Block block = (Fixed12Block) builder.build();
        block.getFixed12First(1); // Only position 0 exists
    }

    @Test
    public void testEncodingName()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 1);
        builder.writeFixed12(100L, 42);
        Block block = builder.build();
        assertEquals(block.getEncodingName(), Fixed12BlockEncoding.NAME);
    }

    @Test
    public void testFixedSizeInBytesPerPosition()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 1);
        builder.writeFixed12(100L, 42);
        Block block = builder.build();
        assertTrue(block.fixedSizeInBytesPerPosition().isPresent());
        assertEquals(block.fixedSizeInBytesPerPosition().getAsInt(), Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 3);
        builder.writeFixed12(100L, 42);
        builder.appendNull();

        Block block = builder.build();
        assertEquals(block.getEstimatedDataSizeForStats(0), Fixed12Block.FIXED12_BYTES);
        assertEquals(block.getEstimatedDataSizeForStats(1), 0);
    }

    @Test
    public void testSliceRoundTripWithWritePositionTo()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(10L, 1);
        builder.appendNull();
        builder.writeFixed12(Long.MAX_VALUE, Integer.MAX_VALUE);
        builder.writeFixed12(0L, 0);
        builder.appendNull();

        Block original = builder.build();

        // Serialize positions individually using writePositionTo/readPositionFrom
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        for (int position = 0; position < original.getPositionCount(); position++) {
            original.writePositionTo(position, (SliceOutput) sliceOutput);
        }

        Slice slice = sliceOutput.slice();
        SliceInput sliceInput = slice.getInput();

        Fixed12BlockBuilder roundTripBuilder = new Fixed12BlockBuilder(null, original.getPositionCount());
        for (int i = 0; i < original.getPositionCount(); i++) {
            roundTripBuilder.readPositionFrom(sliceInput);
        }

        Block roundTripped = roundTripBuilder.build();
        assertBlockEquals(roundTripped, original);
    }

    @Test
    public void testSliceRoundTripNegativeValues()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 3);
        builder.writeFixed12(-1L, 999999);
        builder.writeFixed12(Long.MIN_VALUE, 0);
        builder.writeFixed12(-123456789L, 500000);

        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        for (int position = 0; position < original.getPositionCount(); position++) {
            original.writePositionTo(position, (SliceOutput) sliceOutput);
        }

        SliceInput sliceInput = sliceOutput.slice().getInput();

        Fixed12BlockBuilder roundTripBuilder = new Fixed12BlockBuilder(null, original.getPositionCount());
        for (int i = 0; i < original.getPositionCount(); i++) {
            roundTripBuilder.readPositionFrom(sliceInput);
        }

        Block roundTripped = roundTripBuilder.build();
        assertBlockEquals(roundTripped, original);
    }

    private void assertBlockEquals(Block actual, Block expected)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        for (int i = 0; i < expected.getPositionCount(); i++) {
            assertEquals(actual.isNull(i), expected.isNull(i), "Null mismatch at position " + i);
            if (!expected.isNull(i)) {
                assertEquals(
                        ((Fixed12Block) actual).getFixed12First(i),
                        ((Fixed12Block) expected).getFixed12First(i),
                        "First value mismatch at position " + i);
                assertEquals(
                        ((Fixed12Block) actual).getFixed12Second(i),
                        ((Fixed12Block) expected).getFixed12Second(i),
                        "Second value mismatch at position " + i);
            }
        }
    }
}
