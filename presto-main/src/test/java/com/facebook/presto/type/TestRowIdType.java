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
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.RowIdType.ROW_ID;
import static com.facebook.presto.type.RowIdOperators.castFromVarcharToRowId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowIdType
        extends AbstractTestType
{
    public TestRowIdType()
    {
        super(ROW_ID, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = ROW_ID.createBlockBuilder(null, 1);
        for (int i = 0; i < 10; i++) {
            String rowId = i + ":row_group_id:data_version:2024-02-12:qt_ig_metrics:34dc";
            ROW_ID.writeSlice(blockBuilder, castFromVarcharToRowId(Slices.utf8Slice(rowId)));
        }
        return blockBuilder.build();
    }

    @Test
    public void testIsOrderable()
    {
        assertTrue(ROW_ID.isOrderable());
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

    @Test
    public void testCompareTo()
    {
        testCompare(
                "54:row_group_id:data_version:2024-02-12:qt_ig_metrics:34dc",
                "98:row_group_id:data_version:2024-02-12:qt_ig_metrics:34dc");
    }

    @Test
    public void testComparePartWise()
    {
        testCompare(
                "9:row_group_id:data_version:2024-02-12:qt_ig_metrics:34dc",
                "99:row_group_id:data_version:2024-02-12:qt_ig_metrics:34dc");
    }

    private void testCompare(String rowIdLesser, String rowIdGreater)
    {
        int actualLesser = ROW_ID.compareTo(rowIdAsBlock(rowIdLesser), 0, rowIdAsBlock(rowIdGreater), 0);
        assertTrue(actualLesser < 0);
        int actualGreater = ROW_ID.compareTo(rowIdAsBlock(rowIdGreater), 0, rowIdAsBlock(rowIdLesser), 0);
        assertTrue(actualGreater > 0);
        int actualEquals = ROW_ID.compareTo(rowIdAsBlock(rowIdGreater), 0, rowIdAsBlock(rowIdGreater), 0);
        assertEquals(actualEquals, 0);
    }

    private Block rowIdAsBlock(String value)
    {
        Slice slice = Slices.utf8Slice(value);
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, slice.length());
        ROW_ID.writeSlice(blockBuilder, slice);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        String s = ((Slice) value).toStringUtf8();
        return Slices.utf8Slice("9" + s); // since row number is on left
    }
}
