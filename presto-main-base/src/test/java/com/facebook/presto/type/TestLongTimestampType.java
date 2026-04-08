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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.Fixed12Block;
import com.facebook.presto.common.block.Fixed12BlockBuilder;
import com.facebook.presto.common.type.LongTimestamp;
import com.facebook.presto.common.type.LongTimestampType;
import com.facebook.presto.common.type.TimestampType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_NANOS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_PICOS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestLongTimestampType
{
    @Test
    public void testBasicProperties()
    {
        LongTimestampType type = (LongTimestampType) TimestampType.createTimestampType(9);
        assertEquals(type.getPrecision(), 9);
        assertTrue(type.isLong());
        assertFalse(type.isShort());
        assertEquals(type.getFixedSize(), 12);
        assertTrue(type.isComparable());
        assertTrue(type.isOrderable());
    }

    @Test
    public void testWriteAndRead()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createBlockBuilder(null, 5);
        type.writeObject(builder, new LongTimestamp(100L, 42));
        type.writeObject(builder, new LongTimestamp(200L, 84));
        type.writeObject(builder, new LongTimestamp(300L, 999999));

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 3);

        LongTimestamp ts0 = type.getObject(block, 0);
        assertEquals(ts0.getEpochMicros(), 100L);
        assertEquals(ts0.getPicosOfMicro(), 42);

        LongTimestamp ts1 = type.getObject(block, 1);
        assertEquals(ts1.getEpochMicros(), 200L);
        assertEquals(ts1.getPicosOfMicro(), 84);

        LongTimestamp ts2 = type.getObject(block, 2);
        assertEquals(ts2.getEpochMicros(), 300L);
        assertEquals(ts2.getPicosOfMicro(), 999999);
    }

    @Test
    public void testEqualTo()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_PICOS;
        BlockBuilder builder1 = type.createBlockBuilder(null, 2);
        type.writeObject(builder1, new LongTimestamp(100L, 42));
        Block block1 = builder1.build();

        BlockBuilder builder2 = type.createBlockBuilder(null, 2);
        type.writeObject(builder2, new LongTimestamp(100L, 42));
        Block block2 = builder2.build();

        BlockBuilder builder3 = type.createBlockBuilder(null, 2);
        type.writeObject(builder3, new LongTimestamp(100L, 43));
        Block block3 = builder3.build();

        assertTrue(type.equalTo(block1, 0, block2, 0));
        assertFalse(type.equalTo(block1, 0, block3, 0));
    }

    @Test
    public void testCompareTo()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_PICOS;

        BlockBuilder builder1 = type.createBlockBuilder(null, 1);
        type.writeObject(builder1, new LongTimestamp(100L, 42));
        Block block1 = builder1.build();

        BlockBuilder builder2 = type.createBlockBuilder(null, 1);
        type.writeObject(builder2, new LongTimestamp(100L, 42));
        Block block2 = builder2.build();

        BlockBuilder builder3 = type.createBlockBuilder(null, 1);
        type.writeObject(builder3, new LongTimestamp(100L, 43));
        Block block3 = builder3.build();

        BlockBuilder builder4 = type.createBlockBuilder(null, 1);
        type.writeObject(builder4, new LongTimestamp(200L, 0));
        Block block4 = builder4.build();

        assertEquals(type.compareTo(block1, 0, block2, 0), 0);
        assertTrue(type.compareTo(block1, 0, block3, 0) < 0);
        assertTrue(type.compareTo(block3, 0, block1, 0) > 0);
        assertTrue(type.compareTo(block1, 0, block4, 0) < 0);
        assertTrue(type.compareTo(block4, 0, block1, 0) > 0);
    }

    @Test
    public void testHash()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;

        BlockBuilder builder1 = type.createBlockBuilder(null, 1);
        type.writeObject(builder1, new LongTimestamp(100L, 42));
        Block block1 = builder1.build();

        BlockBuilder builder2 = type.createBlockBuilder(null, 1);
        type.writeObject(builder2, new LongTimestamp(100L, 42));
        Block block2 = builder2.build();

        assertEquals(type.hash(block1, 0), type.hash(block2, 0));
    }

    @Test
    public void testAppendTo()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;

        BlockBuilder sourceBuilder = type.createBlockBuilder(null, 2);
        type.writeObject(sourceBuilder, new LongTimestamp(100L, 42));
        sourceBuilder.appendNull();
        Block source = sourceBuilder.build();

        BlockBuilder targetBuilder = type.createBlockBuilder(null, 2);
        type.appendTo(source, 0, targetBuilder);
        type.appendTo(source, 1, targetBuilder);
        Block target = targetBuilder.build();

        assertEquals(target.getPositionCount(), 2);
        assertFalse(target.isNull(0));
        assertTrue(target.isNull(1));

        LongTimestamp ts = type.getObject(target, 0);
        assertEquals(ts.getEpochMicros(), 100L);
        assertEquals(ts.getPicosOfMicro(), 42);
    }

    @Test
    public void testCreateBlockBuilder()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createBlockBuilder(null, 10);
        assertNotNull(builder);
        assertTrue(builder instanceof Fixed12BlockBuilder);
    }

    @Test
    public void testCreateFixedSizeBlockBuilder()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createFixedSizeBlockBuilder(10);
        assertNotNull(builder);
        assertTrue(builder instanceof Fixed12BlockBuilder);
    }

    @Test
    public void testGetObjectValueNullPosition()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        builder.appendNull();
        Block block = builder.build();

        Object value = type.getObjectValue(null, block, 0);
        assertEquals(value, null);
    }

    @Test
    public void testGetObjectValueWithFixed12Block()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        type.writeObject(builder, new LongTimestamp(12345L, 678));
        Block block = builder.build();
        assertTrue(block instanceof Fixed12Block);

        Object value = type.getObjectValue(null, block, 0);
        assertTrue(value instanceof LongTimestamp);
        LongTimestamp ts = (LongTimestamp) value;
        assertEquals(ts.getEpochMicros(), 12345L);
        assertEquals(ts.getPicosOfMicro(), 678);
    }

    @Test
    public void testNullHandling()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_NANOS;
        BlockBuilder builder = type.createBlockBuilder(null, 3);
        type.writeObject(builder, new LongTimestamp(100L, 42));
        builder.appendNull();
        type.writeObject(builder, new LongTimestamp(300L, 84));

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 3);
        assertFalse(block.isNull(0));
        assertTrue(block.isNull(1));
        assertFalse(block.isNull(2));
    }

    @Test
    public void testMultiplePrecisions()
    {
        for (int precision = 7; precision <= 12; precision++) {
            LongTimestampType type = (LongTimestampType) TimestampType.createTimestampType(precision);
            assertEquals(type.getPrecision(), precision);
            assertTrue(type.isLong());
            assertEquals(type.getFixedSize(), 12);

            // Test write and read
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            type.writeObject(builder, new LongTimestamp(42L, 123));
            Block block = builder.build();

            LongTimestamp ts = type.getObject(block, 0);
            assertEquals(ts.getEpochMicros(), 42L);
            assertEquals(ts.getPicosOfMicro(), 123);
        }
    }

    @Test
    public void testTypeEquality()
    {
        LongTimestampType type9a = (LongTimestampType) TimestampType.createTimestampType(9);
        LongTimestampType type9b = (LongTimestampType) TimestampType.createTimestampType(9);
        LongTimestampType type12 = (LongTimestampType) TimestampType.createTimestampType(12);

        assertEquals(type9a, type9b);
        assertEquals(type9a.hashCode(), type9b.hashCode());
        assertFalse(type9a.equals(type12));
    }

    @Test
    public void testLargeValues()
    {
        LongTimestampType type = (LongTimestampType) TIMESTAMP_PICOS;
        BlockBuilder builder = type.createBlockBuilder(null, 2);
        type.writeObject(builder, new LongTimestamp(Long.MAX_VALUE, 999999));
        type.writeObject(builder, new LongTimestamp(Long.MIN_VALUE, 0));

        Block block = builder.build();

        LongTimestamp ts0 = type.getObject(block, 0);
        assertEquals(ts0.getEpochMicros(), Long.MAX_VALUE);
        assertEquals(ts0.getPicosOfMicro(), 999999);

        LongTimestamp ts1 = type.getObject(block, 1);
        assertEquals(ts1.getEpochMicros(), Long.MIN_VALUE);
        assertEquals(ts1.getPicosOfMicro(), 0);
    }
}
