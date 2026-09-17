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

import com.facebook.presto.common.type.TimestampType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
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
        long epochMicros = 1_000_000_000L;
        int picosOfMicro = 500_000;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getLong(0), epochMicros);
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }

    @Test
    public void testNegativeEpochMicros()
    {
        long epochMicros = -1_000_000L;
        int picosOfMicro = 999_999;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
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
        assertEquals(block.getSizeInBytes(), Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * 4L);
    }

    @Test
    public void testTimestampTypeCreateBlockBuilderForLongPrecision()
    {
        TimestampType longType = createTimestampType(7);
        assertFalse(longType.isShort());
        BlockBuilder builder = longType.createBlockBuilder(null, 10);
        assertTrue(builder instanceof Fixed12ArrayBlockBuilder,
                "Expected Fixed12ArrayBlockBuilder for p=7, got: " + builder.getClass().getSimpleName());
    }

    @Test
    public void testTimestampTypeCreateBlockBuilderForShortPrecision()
    {
        TimestampType shortType = createTimestampType(6);
        assertTrue(shortType.isShort());
        BlockBuilder builder = shortType.createBlockBuilder(null, 10);
        assertTrue(builder instanceof LongArrayBlockBuilder,
                "Expected LongArrayBlockBuilder for p=6, got: " + builder.getClass().getSimpleName());
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
    public void testExtremeNegativeEpoch()
    {
        long epochMicros = Long.MIN_VALUE;
        int picosOfMicro = 0;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }

    @Test
    public void testBuilderGetLongNoOffset()
    {
        long epochMicros = -999_000L;
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(0).closeEntry();

        assertEquals(builder.getLong(0), epochMicros);
        assertEquals(builder.getLong(0, 0), epochMicros);
    }

    @Test
    public void testEqualTo()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(100L).writeInt(999_999).closeEntry();
        Block block = builder.build();

        assertTrue(longType.equalTo(block, 0, block, 0), "value must equal itself");
        assertTrue(longType.equalTo(block, 0, block, 1), "identical values must be equal");
        assertFalse(longType.equalTo(block, 0, block, 2), "same epochMicros but different picosOfMicro must not be equal");
    }

    @Test
    public void testHashEqualsContract()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(42L).writeInt(7).closeEntry();
        builder.writeLong(42L).writeInt(7).closeEntry();
        Block block = builder.build();

        assertTrue(longType.equalTo(block, 0, block, 1));
        assertEquals(longType.hash(block, 0), longType.hash(block, 1),
                "equal values must have the same hash");
    }

    @Test
    public void testHashDifferentiatesByPicosOfMicro()
    {
        // Guards against a future bug where hash() ignores the picosOfMicro field.
        TimestampType nanos = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.writeLong(100L).writeInt(1).closeEntry();
        Block block = builder.build();

        assertNotEquals(nanos.hash(block, 0), nanos.hash(block, 1),
                "same epochMicros but different picosOfMicro must produce different hashes");
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
    public void testWriteIntRejectsPicosOutOfRange()
    {
        expectThrows(IllegalArgumentException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(0L).writeInt(-1));

        expectThrows(IllegalArgumentException.class, () ->
                new Fixed12ArrayBlockBuilder(null, 1).writeLong(0L).writeInt(1_000_000));

        new Fixed12ArrayBlockBuilder(null, 1).writeLong(0L).writeInt(0).closeEntry();
        new Fixed12ArrayBlockBuilder(null, 1).writeLong(0L).writeInt(999_999).closeEntry();
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
    public void testEncodingRejectsOutOfRangePicosOfMicro()
    {
        // Bypass the builder's validation by writing raw bytes directly.
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        out.writeInt(1);          // positionCount
        out.writeBoolean(false);  // mayHaveNull = false (matches encodeNullsAsBits output)
        out.writeLong(0L);        // epochMicros
        out.writeInt(1_000_000);  // invalid picosOfMicro

        Fixed12ArrayBlockEncoding encoding = new Fixed12ArrayBlockEncoding();
        expectThrows(IllegalArgumentException.class,
                () -> encoding.readBlock(null, out.slice().getInput()));
    }

    @Test
    public void testCompareToNegativeEpochWithNonZeroPicos()
    {
        TimestampType nanos = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(-1_000L).writeInt(0).closeEntry();       // earlier
        builder.writeLong(-1_000L).writeInt(500_000).closeEntry(); // same epoch, higher picos
        builder.writeLong(-999L).writeInt(0).closeEntry();         // later epoch
        Block block = builder.build();

        assertEquals(nanos.compareTo(block, 0, block, 0), 0);
        assertTrue(nanos.compareTo(block, 0, block, 1) < 0, "same negative epoch, lower picos must sort first");
        assertTrue(nanos.compareTo(block, 1, block, 0) > 0);
        assertTrue(nanos.compareTo(block, 0, block, 2) < 0, "more negative epoch must sort first");
        assertTrue(nanos.compareTo(block, 2, block, 0) > 0);
    }

    @Test
    public void testWritePositionToBlockBuilderContractForNullPosition()
    {
        // Same contract as LongArrayBlock/Int128ArrayBlock: null positions are written as zeroed non-null entries.
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
