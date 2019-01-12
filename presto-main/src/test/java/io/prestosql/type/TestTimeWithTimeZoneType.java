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
package io.prestosql.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.SqlTimeWithTimeZone;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;

public class TestTimeWithTimeZoneType
        extends AbstractTestType
{
    public TestTimeWithTimeZoneType()
    {
        super(TIME_WITH_TIME_ZONE, SqlTimeWithTimeZone.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIME_WITH_TIME_ZONE.createBlockBuilder(null, 15);
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(0)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(1)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(2)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(3)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(4)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(5)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(6)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(7)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(8)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(9)));
        TIME_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(4444, getTimeZoneKeyForOffset(10)));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        // time zone doesn't matter for ordering
        return packDateTimeWithZone(unpackMillisUtc((Long) value) + 10, getTimeZoneKeyForOffset(33));
    }
}
