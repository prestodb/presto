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

package com.facebook.presto.hive;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.VarbinaryType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestRowIDCoercer
{
    private HiveCoercer coercer;
    private final byte[] rowIdPartitionComponent = {(byte) 8, (byte) 9};

    @BeforeMethod
    public void setUp()
    {
        coercer = new RowIDCoercer(rowIdPartitionComponent);
    }

    @Test
    public void testGetToType()
    {
        assertEquals(coercer.getToType(), VarbinaryType.VARBINARY);
    }

    @Test
    public void testApply()
    {
        Block rowNumbers = new LongArrayBlockBuilder(null, 5)
                .writeLong(Long.MAX_VALUE)
                .writeLong(Long.MIN_VALUE)
                .writeLong(0L)
                .writeLong(1L)
                .writeLong(-1L)
                .build();
        Block rowIDs = coercer.apply(rowNumbers);
        assertEquals(rowIDs.getPositionCount(), rowNumbers.getPositionCount());
        byte[] firstRowId = rowIDs.getSlice(0, 0, rowIDs.getSliceLength(0)).getBytes();
    }
}
