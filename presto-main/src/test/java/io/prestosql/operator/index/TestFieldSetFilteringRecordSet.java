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
package io.prestosql.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.util.StructuralTestUtil.arrayBlockOf;
import static org.testng.Assert.assertTrue;

public class TestFieldSetFilteringRecordSet
{
    @Test
    public void test()
    {
        TypeManager typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());

        ArrayType arrayOfBigintType = new ArrayType(BIGINT);
        FieldSetFilteringRecordSet fieldSetFilteringRecordSet = new FieldSetFilteringRecordSet(
                functionRegistry,
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
                                        arrayBlockOf(BIGINT, 12, 34, 56)))),
                ImmutableList.of(ImmutableSet.of(0, 1), ImmutableSet.of(2, 3), ImmutableSet.of(4, 5)));
        RecordCursor recordCursor = fieldSetFilteringRecordSet.cursor();
        assertTrue(recordCursor.advanceNextPosition());
    }
}
