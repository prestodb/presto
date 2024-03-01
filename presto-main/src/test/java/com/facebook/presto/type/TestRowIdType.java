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
import com.facebook.presto.common.type.SqlVarbinary;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.facebook.presto.common.type.RowIdType.ROW_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRowIdType
        extends AbstractTestType
{
    public TestRowIdType()
    {
        super(ROW_ID, SqlVarbinary.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = ROW_ID.createBlockBuilder(null, 1);
        for (int i = 0; i < 10; i++) {
            ByteBuffer rowId = ByteBuffer.allocate(4 + 100 + 4 + 8 + 8 + 4 + 2)
                    // row number
                    .putInt(i)
                    // row group ID
                    .put("some row Group ID".getBytes(StandardCharsets.UTF_8))
                    // data version
                    .putInt(3249786)
                    // internal partition id
                    .putLong(78645)
                    // table id
                    .putLong(9487435789L)
                    // 6 digits of table guid (hex string), converted to integer
                    .putInt(Integer.parseInt("A9F054", 16))
                    // row-id version
                    .putShort((short) 0);
            rowId.flip(); // needed?
            ROW_ID.writeSlice(blockBuilder, Slices.wrappedBuffer(rowId));
        }
        return blockBuilder.build();
    }

    @Test
    public void testIsOrderable()
    {
        assertFalse(ROW_ID.isOrderable());
    }

    @Test
    public void testIsComparable()
    {
        assertTrue(ROW_ID.isComparable());
    }

    @Test
    public void testDisplayName()
    {
        assertEquals(ROW_ID.getDisplayName(), "Row ID");
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return null;
    }
}
