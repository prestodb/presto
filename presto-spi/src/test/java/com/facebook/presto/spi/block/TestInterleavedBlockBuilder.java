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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestInterleavedBlockBuilder
{
    private static final int VARCHAR_VALUE_SIZE = 7;
    // InterleavedBlockBuilder: value_bigint (FixedWidthBlockBuilder: isNull, value), value_varchar (VariableWidthBlockBuilder: isNull, offset, value)
    private static final int INT_VARCHAR_ENTRY_SIZE = (Byte.BYTES + Long.BYTES) + (Byte.BYTES + Integer.BYTES + VARCHAR_VALUE_SIZE);
    private static final int EXPECTED_ENTRY_COUNT = 100;

    @Test
    public void testArrayBlockIsFull()
            throws Exception
    {
        // divide by two because INT_VARCHAR_ENTRY_SIZE is the size of two entries.
        testIsFull(new PageBuilderStatus(INT_VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT / 2, 10240));
        testIsFull(new PageBuilderStatus(10240, INT_VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT / 2));
    }

    private void testIsFull(PageBuilderStatus pageBuilderStatus)
    {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BIGINT, VARCHAR), pageBuilderStatus.createBlockBuilderStatus(), EXPECTED_ENTRY_COUNT);
        assertTrue(pageBuilderStatus.isEmpty());
        while (!pageBuilderStatus.isFull()) {
            BIGINT.writeLong(blockBuilder, 12);
            VARCHAR.writeSlice(blockBuilder, Slices.allocate(VARCHAR_VALUE_SIZE));
        }
        assertEquals(blockBuilder.getPositionCount(), EXPECTED_ENTRY_COUNT);
        assertEquals(pageBuilderStatus.isFull(), true);
    }
}
