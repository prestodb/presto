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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestVariableWidthBlockEncoding
{
    public static final SqlFunctionProperties PROPERTIES = SqlFunctionProperties.builder().setTimeZoneKey(UTC_KEY).setLegacyTimestamp(true).setSessionStartTime(0).setSessionLocale(ENGLISH).setSessionUser("user").build();
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Test
    public void testRoundTrip()
    {
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(PROPERTIES, actual, position), type.getObjectValue(PROPERTIES, expected, position));
        }
    }
}
