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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestTimestampType
        extends AbstractTestType
{
    public TestTimestampType()
    {
        super(TIMESTAMP, SqlTimestamp.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP.createBlockBuilder(null, 15);
        TIMESTAMP.writeLong(blockBuilder, 1111);
        TIMESTAMP.writeLong(blockBuilder, 1111);
        TIMESTAMP.writeLong(blockBuilder, 1111);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 3333);
        TIMESTAMP.writeLong(blockBuilder, 3333);
        TIMESTAMP.writeLong(blockBuilder, 4444);
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
        TimestampType timeStampType = TIMESTAMP;
        assertEquals(timeStampType, TIMESTAMP);
        assertEquals(timeStampType.hashCode(), TIMESTAMP.hashCode());

        assertNotEquals(timeStampType, TIMESTAMP_MICROSECONDS);
    }
}
