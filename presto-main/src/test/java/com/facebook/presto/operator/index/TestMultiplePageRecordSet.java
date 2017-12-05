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
package com.facebook.presto.operator.index;

import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMultiplePageRecordSet
{
    @Test
    public void testRecordSetConversionRoundTrip()
    {
        ArrayType arrayOfBigintType = new ArrayType(BIGINT);
        RecordSet recordSet = new InMemoryRecordSet(
                ImmutableList.of(BIGINT, BIGINT, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, arrayOfBigintType, arrayOfBigintType),
                ImmutableList.of(
                        ImmutableList.of(
                                100L,
                                100L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                101L,
                                100L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                102L,
                                102L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(103, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                103L,
                                103L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56))));
        PageSet pageSet = SimplePageSet.fromRecordSet(recordSet);
        RecordSet recordSet1 = new MultiplePageRecordSet(pageSet);
        RecordCursor cursor = recordSet1.cursor();
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 100L);
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 101L);
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 102L);
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 103L);
        assertFalse(cursor.advanceNextPosition());
        assertRecordSetEquals(recordSet, recordSet1);
    }

    private void assertRecordSetEquals(RecordSet actual, RecordSet expected)
    {
        assertEquals(actual.getColumnTypes(), expected.getColumnTypes());
        RecordCursor actualCursor = actual.cursor();
        RecordCursor expectedCursor = expected.cursor();
        List<Type> typeList = actual.getColumnTypes();
        while (actualCursor.advanceNextPosition() && expectedCursor.advanceNextPosition()) {
            for (int i = 0; i < typeList.size(); i++) {
                Type type = typeList.get(i);
                Class<?> javaType = type.getJavaType();
                if (javaType == Long.class) {
                    assertEquals(actualCursor.getLong(i), expectedCursor.getLong(i));
                }
            }
        }
    }
}
