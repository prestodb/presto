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
package com.facebook.presto.spi.block;

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVariableWidthBlockBuilder
{
    private static final int VARCHAR_ENTRY_OVERHEAD = SIZE_OF_BYTE + SIZE_OF_INT;
    private static final int VARCHAR_VALUE_SIZE = 7;
    private static final int VARCHAR_ENTRY_SIZE = VARCHAR_ENTRY_OVERHEAD + VARCHAR_VALUE_SIZE;
    private static final int EXPECTED_ENTRY_COUNT = 3;

    @Test
    public void testFixedBlockIsFull()
            throws Exception
    {
        testIsFull(new VariableWidthBlockBuilder(VARCHAR, new BlockBuilderStatus(VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT, 1024)));
        testIsFull(new VariableWidthBlockBuilder(VARCHAR, new BlockBuilderStatus(1024, VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT)));
    }

    private void testIsFull(VariableWidthBlockBuilder blockBuilder)
    {
        assertTrue(blockBuilder.isEmpty());
        while (!blockBuilder.isFull()) {
            blockBuilder.append(new byte[VARCHAR_VALUE_SIZE]);
        }
        assertEquals(blockBuilder.getPositionCount(), EXPECTED_ENTRY_COUNT);
        assertEquals(blockBuilder.isFull(), true);
    }
}
