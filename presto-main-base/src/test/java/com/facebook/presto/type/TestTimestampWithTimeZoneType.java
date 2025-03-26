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
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

public class TestTimestampWithTimeZoneType
        extends AbstractTestType
{
    public TestTimestampWithTimeZoneType()
    {
        super(TIMESTAMP_WITH_TIME_ZONE, SqlTimestampWithTimeZone.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_WITH_TIME_ZONE.createBlockBuilder(null, 15);
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(0)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(1)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(2)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(3)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(4)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(5)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(6)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(7)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(8)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(9)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(4444, getTimeZoneKeyForOffset(10)));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        // time zone doesn't matter for ordering
        return packDateTimeWithZone(unpackMillisUtc((Long) value) + 10, getTimeZoneKeyForOffset(33));
    }
}
