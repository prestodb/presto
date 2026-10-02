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
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.common.block.Fixed12ArrayBlock.FIXED12_BYTES;
import static com.facebook.presto.common.block.Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestFixed12ArrayBlock
{
    @Test
    public void testSinglePositionRoundTrip()
    {
        long longValue = 1_000_000_000L;
        int intValue = 500_000;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(longValue).writeInt(intValue).closeEntry();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getLong(0), longValue);
        assertEquals(block.getLong(0, 0), longValue);
        assertEquals(block.getInt(0), intValue);
    }

    @Test
    public void testNegativeLongComponent()
    {
        long longValue = -1_000_000L;
        int intValue = 999_999;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(longValue).writeInt(intValue).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), longValue);
        assertEquals(block.getInt(0), intValue);
    }

    @Test
    public void testMultiplePositions()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.writeLong(200L).writeInt(999_999).closeEntry();
        builder.writeLong(-300L).writeInt(500_000).closeEntry();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 3);
        assertEquals(block.getLong(0, 0), 100L);
        assertEquals(block.getInt(0), 0);
        assertEquals(block.getLong(1, 0), 200L);
        assertEquals(block.getInt(1), 999_999);
        assertEquals(block.getLong(2, 0), -300L);
        assertEquals(block.getInt(2), 500_000);
    }

    @Test
    public void testNullPosition()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.appendNull();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 2);
        assertFalse(block.isNull(0));
        assertTrue(block.isNull(1));
    }

    @Test
    public void testSizeInBytes()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 4);
        for (int i = 0; i < 4; i++) {
            builder.writeLong((long) i * 1000).writeInt(i).closeEntry();
        }
        Block block = builder.build();
        assertEquals(block.getSizeInBytes(), SIZE_IN_BYTES_PER_POSITION * 4L);
    }

    @Test
    public void testEncodingRoundTrip()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(1_000_000_000L).writeInt(500_000).closeEntry();
        builder.appendNull();
        builder.writeLong(-999_999L).writeInt(999_999).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        Fixed12ArrayBlockEncoding encoding = new Fixed12ArrayBlockEncoding();
        encoding.writeBlock(null, sliceOutput, original);

        SliceInput sliceInput = sliceOutput.slice().getInput();
        Block decoded = encoding.readBlock(null, sliceInput);

        assertEquals(decoded.getPositionCount(), 3);
        assertFalse(decoded.isNull(0));
        assertEquals(decoded.getLong(0, 0), 1_000_000_000L);
        assertEquals(decoded.getInt(0), 500_000);
        assertTrue(decoded.isNull(1));
        assertFalse(decoded.isNull(2));
        assertEquals(decoded.getLong(2, 0), -999_999L);
        assertEquals(decoded.getInt(2), 999_999);
    }

    @Test
    public void testPackingOrderIsLowWordFirst()
    {
        // The int[] constructor is public, so the packing order is part of the API: the low 32-bit
        // word of the long occupies the first slot, matching the little-endian wire layout.
        Block block = new Fixed12ArrayBlock(1, Optional.empty(), new int[] {0x89ABCDEF, 0x01234567, 7});

        assertEquals(block.getLong(0, 0), 0x0123456789ABCDEFL);
        assertEquals(block.getInt(0), 7);
    }

    @Test
    public void testEncodingRoundTripWithoutNulls()
    {
        // A null-free block decodes through the bulk read path. The long components below have
        // distinct high and low words, so a packing order that disagrees with the wire layout
        // would corrupt them.
        long[] longValues = {0x0123456789ABCDEFL, Long.MIN_VALUE, -1L};

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, longValues.length);
        for (int i = 0; i < longValues.length; i++) {
            builder.writeLong(longValues[i]).writeInt(i).closeEntry();
        }
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        Fixed12ArrayBlockEncoding encoding = new Fixed12ArrayBlockEncoding();
        encoding.writeBlock(null, sliceOutput, original);
        Block decoded = encoding.readBlock(null, sliceOutput.slice().getInput());

        assertEquals(decoded.getPositionCount(), longValues.length);
        assertFalse(decoded.mayHaveNull());
        for (int i = 0; i < longValues.length; i++) {
            assertEquals(decoded.getLong(i, 0), longValues[i]);
            assertEquals(decoded.getInt(i), i);
        }
    }

    @Test
    public void testLongComponentMinValue()
    {
        long longValue = Long.MIN_VALUE;
        int intValue = 0;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(longValue).writeInt(intValue).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), longValue);
        assertEquals(block.getInt(0), intValue);
    }

    @Test
    public void testBuilderGetLongNoOffset()
    {
        long longValue = -999_000L;
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(longValue).writeInt(0).closeEntry();

        assertEquals(builder.getLong(0), longValue);
        assertEquals(builder.getLong(0, 0), longValue);
    }

    @Test
    public void testGetRegionWithNonZeroOffset()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 5);
        for (int i = 0; i < 5; i++) {
            builder.writeLong((long) (i + 1) * 100).writeInt(i * 10).closeEntry();
        }
        Block block = builder.build();

        Block region1 = block.getRegion(1, 4);
        assertEquals(region1.getPositionCount(), 4);
        assertEquals(region1.getLong(0, 0), 200L);
        assertEquals(region1.getInt(0), 10);

        Block region2 = region1.getRegion(1, 2);
        assertEquals(region2.getPositionCount(), 2);
        assertEquals(region2.getLong(0, 0), 300L);
        assertEquals(region2.getInt(0), 20);
        assertEquals(region2.getLong(1, 0), 400L);
        assertEquals(region2.getInt(1), 30);
    }

    @Test
    public void testCopyRegionWithNonZeroOffset()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 5);
        for (int i = 0; i < 5; i++) {
            builder.writeLong((long) (i + 1) * 100).writeInt(i * 10).closeEntry();
        }
        Block block = builder.build();

        Block region = block.getRegion(2, 3);
        Block copy = region.copyRegion(0, 2);

        assertEquals(copy.getPositionCount(), 2);
        assertEquals(copy.getLong(0, 0), 300L);
        assertEquals(copy.getInt(0), 20);
        assertEquals(copy.getLong(1, 0), 400L);
        assertEquals(copy.getInt(1), 30);
        assertEquals(copy.getLong(0, 0), region.getLong(0, 0));
    }

    @Test
    public void testEncodingRoundTripViaSerde()
    {
        BlockEncodingSerde serde = new TestingBlockEncodingSerde();

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(1_000_000_000L).writeInt(500_000).closeEntry();
        builder.appendNull();
        builder.writeLong(-999_999L).writeInt(999_999).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        serde.writeBlock(sliceOutput, original);
        Block decoded = serde.readBlock(sliceOutput.slice().getInput());

        assertEquals(decoded.getPositionCount(), 3);
        assertFalse(decoded.isNull(0));
        assertEquals(decoded.getLong(0, 0), 1_000_000_000L);
        assertEquals(decoded.getInt(0), 500_000);
        assertTrue(decoded.isNull(1));
        assertFalse(decoded.isNull(2));
        assertEquals(decoded.getLong(2, 0), -999_999L);
        assertEquals(decoded.getInt(2), 999_999);
    }

    @Test
    public void testWriteProtocolViolations()
    {
        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeInt(0));

        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).closeEntry());

        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(1L).closeEntry());

        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(1L).appendNull());

        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(1L).writeLong(2L));

        expectThrows(IllegalStateException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(1L).build());
    }

    @Test
    public void testStoresArbitraryIntComponent()
    {
        // The block is type-agnostic: it stores whatever int it is given, including negative values
        // and the full int range. Range checks belong to the Type that owns the layout - see
        // LongTimestamp, which validates picosOfMicro before LongTimestampType writes it here.
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 4);
        builder.writeLong(0L).writeInt(Integer.MIN_VALUE).closeEntry();
        builder.writeLong(0L).writeInt(-1).closeEntry();
        builder.writeLong(0L).writeInt(0).closeEntry();
        builder.writeLong(0L).writeInt(Integer.MAX_VALUE).closeEntry();
        Block block = builder.build();

        assertEquals(block.getInt(0), Integer.MIN_VALUE);
        assertEquals(block.getInt(1), -1);
        assertEquals(block.getInt(2), 0);
        assertEquals(block.getInt(3), Integer.MAX_VALUE);
    }

    @Test
    public void testEncodingRoundTripViaBlockEncodingManager()
    {
        BlockEncodingSerde serde = new BlockEncodingManager();

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(1_000_000_000L).writeInt(500_000).closeEntry();
        builder.appendNull();
        builder.writeLong(-999_999L).writeInt(999_999).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        serde.writeBlock(sliceOutput, original);
        Block decoded = serde.readBlock(sliceOutput.slice().getInput());

        assertEquals(decoded.getPositionCount(), 3);
        assertTrue(decoded instanceof Fixed12ArrayBlock, "Expected Fixed12ArrayBlock, got: " + decoded.getClass().getSimpleName());
        assertFalse(decoded.isNull(0));
        assertEquals(decoded.getLong(0, 0), 1_000_000_000L);
        assertEquals(decoded.getInt(0), 500_000);
        assertTrue(decoded.isNull(1));
        assertFalse(decoded.isNull(2));
        assertEquals(decoded.getLong(2, 0), -999_999L);
        assertEquals(decoded.getInt(2), 999_999);
    }

    @Test
    public void testAppendNullOnBlock()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(200L).writeInt(0).closeEntry();
        Block base = builder.build();

        Block withNull = base.appendNull();
        assertEquals(withNull.getPositionCount(), 3);
        assertFalse(withNull.isNull(0));
        assertFalse(withNull.isNull(1));
        assertTrue(withNull.isNull(2));
        assertEquals(withNull.getLong(0, 0), 100L);
        assertEquals(withNull.getInt(0), 500_000);
    }

    @Test
    public void testEncodingRoundTripsFullIntRange()
    {
        // The encoding is type-agnostic too: it round-trips whatever the block holds.
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(Long.MIN_VALUE).writeInt(Integer.MIN_VALUE).closeEntry();
        builder.writeLong(Long.MAX_VALUE).writeInt(Integer.MAX_VALUE).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput out = new DynamicSliceOutput(64);
        Fixed12ArrayBlockEncoding encoding = new Fixed12ArrayBlockEncoding();
        encoding.writeBlock(null, out, original);
        Block decoded = encoding.readBlock(null, out.slice().getInput());

        assertEquals(decoded.getPositionCount(), 2);
        assertEquals(decoded.getLong(0, 0), Long.MIN_VALUE);
        assertEquals(decoded.getInt(0), Integer.MIN_VALUE);
        assertEquals(decoded.getLong(1, 0), Long.MAX_VALUE);
        assertEquals(decoded.getInt(1), Integer.MAX_VALUE);
    }

    @Test
    public void testWritePositionToBlockBuilderContractForNullPosition()
    {
        // The zeroed output below is incidental (backing int[] defaults to 0), not a guarantee
        // of Block.writePositionTo's contract.
        Fixed12ArrayBlockBuilder source = new Fixed12ArrayBlockBuilder(null, 2);
        source.writeLong(42L).writeInt(7).closeEntry();
        source.appendNull();
        Block sourceBlock = source.build();

        Fixed12ArrayBlockBuilder dest = new Fixed12ArrayBlockBuilder(null, 2);
        sourceBlock.writePositionTo(0, dest);
        sourceBlock.writePositionTo(1, dest);

        assertEquals(dest.getPositionCount(), 2);
        assertFalse(dest.isNull(0));
        assertEquals(dest.getLong(0, 0), 42L);
        assertEquals(dest.getInt(0), 7);
        assertFalse(dest.isNull(1)); // null position written as non-null per contract
        assertEquals(dest.getLong(1, 0), 0L);
        assertEquals(dest.getInt(1), 0);
    }

    @Test
    public void testWritePositionToSliceOutputRoundTrip()
    {
        Fixed12ArrayBlockBuilder source = new Fixed12ArrayBlockBuilder(null, 3);
        source.writeLong(1_000_000L).writeInt(500_000).closeEntry();
        source.appendNull();
        source.writeLong(-999L).writeInt(999_999).closeEntry();
        Block sourceBlock = source.build();

        DynamicSliceOutput out = new DynamicSliceOutput(256);
        for (int i = 0; i < sourceBlock.getPositionCount(); i++) {
            sourceBlock.writePositionTo(i, out);
        }

        SliceInput in = out.slice().getInput();
        Fixed12ArrayBlockBuilder dest = new Fixed12ArrayBlockBuilder(null, 3);
        for (int i = 0; i < sourceBlock.getPositionCount(); i++) {
            dest.readPositionFrom(in);
        }
        Block result = dest.build();

        assertEquals(result.getPositionCount(), 3);
        assertFalse(result.isNull(0));
        assertEquals(result.getLong(0, 0), 1_000_000L);
        assertEquals(result.getInt(0), 500_000);
        assertTrue(result.isNull(1));
        assertFalse(result.isNull(2));
        assertEquals(result.getLong(2, 0), -999L);
        assertEquals(result.getInt(2), 999_999);
    }

    @Test
    public void testGetSingleValueBlock()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(42L).writeInt(7).closeEntry();
        builder.appendNull();
        builder.writeLong(-999L).writeInt(999_999).closeEntry();
        Block block = builder.build();

        Block single0 = block.getSingleValueBlock(0);
        assertEquals(single0.getPositionCount(), 1);
        assertFalse(single0.isNull(0));
        assertEquals(single0.getLong(0, 0), 42L);
        assertEquals(single0.getInt(0), 7);

        Block single1 = block.getSingleValueBlock(1);
        assertEquals(single1.getPositionCount(), 1);
        assertTrue(single1.isNull(0));

        Block single2 = block.getSingleValueBlock(2);
        assertEquals(single2.getPositionCount(), 1);
        assertFalse(single2.isNull(0));
        assertEquals(single2.getLong(0, 0), -999L);
        assertEquals(single2.getInt(0), 999_999);
    }

    @Test
    public void testCopyPositions()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 4);
        builder.writeLong(10L).writeInt(100).closeEntry();
        builder.writeLong(20L).writeInt(200).closeEntry();
        builder.appendNull();
        builder.writeLong(40L).writeInt(400).closeEntry();
        Block block = builder.build();

        int[] positions = {3, 0, 2};
        Block copy = block.copyPositions(positions, 0, 3);

        assertEquals(copy.getPositionCount(), 3);
        assertFalse(copy.isNull(0));
        assertEquals(copy.getLong(0, 0), 40L);
        assertEquals(copy.getInt(0), 400);
        assertFalse(copy.isNull(1));
        assertEquals(copy.getLong(1, 0), 10L);
        assertEquals(copy.getInt(1), 100);
        assertTrue(copy.isNull(2));
    }

    @Test
    public void testEqualsAndHashCode()
    {
        Fixed12ArrayBlockBuilder builder1 = new Fixed12ArrayBlockBuilder(null, 2);
        builder1.writeLong(100L).writeInt(500_000).closeEntry();
        builder1.appendNull();
        Block block1 = builder1.build();

        Fixed12ArrayBlockBuilder builder2 = new Fixed12ArrayBlockBuilder(null, 2);
        builder2.writeLong(100L).writeInt(500_000).closeEntry();
        builder2.appendNull();
        Block block2 = builder2.build();

        assertEquals(block1, block2);
        assertEquals(block1.hashCode(), block2.hashCode());
        assertEquals(block1, block1);

        Fixed12ArrayBlockBuilder builder3 = new Fixed12ArrayBlockBuilder(null, 2);
        builder3.writeLong(100L).writeInt(500_001).closeEntry();
        builder3.appendNull();
        Block block3 = builder3.build();

        assertNotEquals(block1, block3);
        assertNotEquals(block1, null);
        assertNotEquals(block1, "not a block");
    }

    @Test
    public void testEqualsWithDifferentPositionOffset()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(100L).writeInt(1).closeEntry();
        builder.writeLong(100L).writeInt(1).closeEntry();
        builder.writeLong(200L).writeInt(2).closeEntry();
        Block block = builder.build();

        Block region1 = block.getRegion(0, 2);
        Block region2 = block.getRegion(1, 2);

        assertNotEquals(region1, region2);
    }

    @Test
    public void testEqualsComparesPhysicalLayout()
    {
        // Equality is physical: a builder block whose backing array is over-allocated is not equal
        // to a compacted block holding the same logical value.
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 8);
        builder.writeLong(100L).writeInt(1).closeEntry();
        Block overAllocated = builder.build();

        Block compact = new Fixed12ArrayBlock(1, Optional.empty(), new int[] {100, 0, 1});

        assertEquals(overAllocated.getPositionCount(), compact.getPositionCount());
        assertEquals(overAllocated.getLong(0, 0), compact.getLong(0, 0));
        assertEquals(overAllocated.getInt(0), compact.getInt(0));
        assertNotEquals(overAllocated, compact);
    }

    @Test
    public void testGrowCapacityKeepsValuesAndNullsInSync()
    {
        // An initial capacity of 1 forces repeated growth, where values must gain three int slots
        // for every valueIsNull slot.
        int entries = 100;
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        for (int i = 0; i < entries; i++) {
            if (i % 3 == 0) {
                builder.appendNull();
            }
            else {
                builder.writeLong(Long.MIN_VALUE + i).writeInt(i).closeEntry();
            }
        }

        Block block = builder.build();
        assertEquals(block.getPositionCount(), entries);
        for (int i = 0; i < entries; i++) {
            if (i % 3 == 0) {
                assertTrue(block.isNull(i));
            }
            else {
                assertFalse(block.isNull(i));
                assertEquals(block.getLong(i, 0), Long.MIN_VALUE + i);
                assertEquals(block.getInt(i), i);
            }
        }
    }

    @Test
    public void testUncheckedAccessorsOnBuilder()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(10L).writeInt(100).closeEntry();
        builder.appendNull();
        builder.writeLong(-30L).writeInt(300).closeEntry();

        assertEquals(builder.getOffsetBase(), 0);
        assertEquals(builder.getLongUnchecked(0), 10L);
        assertEquals(builder.getLongUnchecked(0, 0), 10L);
        assertEquals(builder.getIntUnchecked(0), 100);
        assertEquals(builder.getLongUnchecked(2), -30L);
        assertEquals(builder.getIntUnchecked(2), 300);
        assertFalse(builder.isNullUnchecked(0));
        assertTrue(builder.isNullUnchecked(1));
    }

    @Test
    public void testUncheckedAccessorsOnBlockAndRegion()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(10L).writeInt(100).closeEntry();
        builder.appendNull();
        builder.writeLong(-30L).writeInt(300).closeEntry();
        Block block = builder.build();

        assertEquals(block.getOffsetBase(), 0);
        assertEquals(block.getLongUnchecked(0), 10L);
        assertEquals(block.getLongUnchecked(0, 0), 10L);
        assertEquals(block.getIntUnchecked(0), 100);
        assertTrue(block.isNullUnchecked(1));

        // A region shifts getOffsetBase(), and unchecked indexing is relative to it.
        Block region = block.getRegion(1, 2);
        assertEquals(region.getOffsetBase(), 1);
        assertTrue(region.isNullUnchecked(1));
        assertFalse(region.isNullUnchecked(2));
        assertEquals(region.getLongUnchecked(2), -30L);
        assertEquals(region.getLongUnchecked(2, 0), -30L);
        assertEquals(region.getIntUnchecked(2), 300);
    }

    @Test
    public void testBlockBuilderStatusByteAccounting()
    {
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus();
        BlockBuilderStatus blockBuilderStatus = pageBuilderStatus.createBlockBuilderStatus();
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(blockBuilderStatus, 4);
        assertTrue(pageBuilderStatus.isEmpty());

        // A partially written entry costs nothing until closeEntry commits it.
        builder.writeLong(1L).writeInt(1);
        assertTrue(pageBuilderStatus.isEmpty());

        builder.closeEntry();
        builder.appendNull();

        // A null costs a full position, same as a value.
        assertEquals(pageBuilderStatus.getSizeInBytes(), 2L * SIZE_IN_BYTES_PER_POSITION);
        assertTrue(builder.getRetainedSizeInBytes() > BlockBuilderStatus.INSTANCE_SIZE);
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(1L).writeInt(1).closeEntry();
        builder.writeLong(2L).writeInt(2).closeEntry();

        BlockBuilder like = builder.newBlockBuilderLike(null);
        assertTrue(like instanceof Fixed12ArrayBlockBuilder);
        assertEquals(like.getPositionCount(), 0);
        like.writeLong(7L).writeInt(70).closeEntry();
        assertEquals(like.getLong(0, 0), 7L);
        assertEquals(like.getInt(0), 70);

        BlockBuilder likeWithExpectedEntries = builder.newBlockBuilderLike(null, 1024);
        assertTrue(likeWithExpectedEntries instanceof Fixed12ArrayBlockBuilder);
        assertEquals(likeWithExpectedEntries.getPositionCount(), 0);
    }

    @Test
    public void testSizeAccountingOnBlock()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(1L).writeInt(1).closeEntry();
        builder.appendNull();
        Block block = builder.build();

        assertEquals(block.fixedSizeInBytesPerPosition(), OptionalInt.of(SIZE_IN_BYTES_PER_POSITION));
        assertEquals(block.getRegionSizeInBytes(0, 1), (long) SIZE_IN_BYTES_PER_POSITION);
        assertEquals(block.getPositionsSizeInBytes(new boolean[] {true, false}, 1), (long) SIZE_IN_BYTES_PER_POSITION);
        assertEquals(block.getEstimatedDataSizeForStats(0), FIXED12_BYTES);
        assertEquals(block.getEstimatedDataSizeForStats(1), 0L);

        List<Object> parts = new ArrayList<>();
        AtomicLong reportedBytes = new AtomicLong();
        block.retainedBytesForEachPart((part, size) -> {
            parts.add(part);
            reportedBytes.addAndGet(size);
        });
        // values, valueIsNull and the block itself.
        assertEquals(parts.size(), 3);
        assertEquals(reportedBytes.get(), block.getRetainedSizeInBytes());
    }

    @Test
    public void testSizeAccountingOnBuilder()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(1L).writeInt(1).closeEntry();
        builder.appendNull();

        assertEquals(builder.fixedSizeInBytesPerPosition(), OptionalInt.of(SIZE_IN_BYTES_PER_POSITION));
        assertEquals(builder.getSizeInBytes(), 2L * SIZE_IN_BYTES_PER_POSITION);
        assertEquals(builder.getRegionSizeInBytes(0, 2), 2L * SIZE_IN_BYTES_PER_POSITION);
        assertEquals(builder.getPositionsSizeInBytes(new boolean[] {true, true}, 2), 2L * SIZE_IN_BYTES_PER_POSITION);
        assertEquals(builder.getEstimatedDataSizeForStats(0), FIXED12_BYTES);
        assertEquals(builder.getEstimatedDataSizeForStats(1), 0L);

        List<Object> parts = new ArrayList<>();
        AtomicLong reportedBytes = new AtomicLong();
        builder.retainedBytesForEachPart((part, size) -> {
            parts.add(part);
            reportedBytes.addAndGet(size);
        });
        assertEquals(parts.size(), 3);
        assertEquals(reportedBytes.get(), builder.getRetainedSizeInBytes());
    }

    @Test
    public void testDictionaryBlockDelegatesLongAndInt()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(100L).writeInt(111).closeEntry();
        builder.writeLong(200L).writeInt(222).closeEntry();
        builder.writeLong(300L).writeInt(333).closeEntry();
        Block dictionary = builder.build();

        int[] ids = {2, 0, 1};
        Block dict = new DictionaryBlock(dictionary, ids);

        assertEquals(dict.getLong(0, 0), 300L);
        assertEquals(dict.getInt(0), 333);
        assertEquals(dict.getLong(1, 0), 100L);
        assertEquals(dict.getInt(1), 111);
        assertEquals(dict.getLong(2, 0), 200L);
        assertEquals(dict.getInt(2), 222);
    }
}
