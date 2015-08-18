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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static org.testng.Assert.assertTrue;

public class TestFieldSetFilteringRecordSet
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final FunctionRegistry FUNCTION_REGISTRY = new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), false);

    @Test
    public void test()
    {
        ArrayType arrayOfBigintType = new ArrayType(BIGINT);
        FieldSetFilteringRecordSet fieldSetFilteringRecordSet = new FieldSetFilteringRecordSet(
                FUNCTION_REGISTRY,
                new InMemoryRecordSet(
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
                                        arrayBlockOf(BIGINT, 12, 34, 56)
                                ))),
                ImmutableList.of(ImmutableSet.of(0, 1), ImmutableSet.of(2, 3), ImmutableSet.of(4, 5)));
        RecordCursor recordCursor = fieldSetFilteringRecordSet.cursor();
        assertTrue(recordCursor.advanceNextPosition());
    }
}
