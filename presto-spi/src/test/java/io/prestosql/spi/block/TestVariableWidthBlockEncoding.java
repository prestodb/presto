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
package io.prestosql.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestVariableWidthBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(new TestingTypeManager());

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
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
