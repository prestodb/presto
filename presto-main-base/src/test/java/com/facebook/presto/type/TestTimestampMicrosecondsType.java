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
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.TimestampType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestTimestampMicrosecondsType
        extends AbstractTestType
{
    public TestTimestampMicrosecondsType()
    {
        super(TIMESTAMP_MICROSECONDS, SqlTimestamp.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_MICROSECONDS.createBlockBuilder(null, 15);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, MIN_VALUE);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, MIN_VALUE + 1_000_000);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 0);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, 1_000_000_000_000L);
        TIMESTAMP_MICROSECONDS.writeLong(blockBuilder, MAX_VALUE - 1);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }

    @Test
    public void testEqualsHashcode()
    {
        TimestampType timeStampType = TIMESTAMP_MICROSECONDS;
        assertEquals(timeStampType, TIMESTAMP_MICROSECONDS);
        assertEquals(timeStampType.hashCode(), TIMESTAMP_MICROSECONDS.hashCode());

        assertNotEquals(timeStampType, TIMESTAMP);
    }
}
