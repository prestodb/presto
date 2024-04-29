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
import com.google.common.primitives.Longs;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TestRowIDCoercer
{
    private final byte[] rowIdPartitionComponent = {(byte) 8, (byte) 9};
    private HiveCoercer coercer = new RowIDCoercer(rowIdPartitionComponent, "some_filename.dat");

    @Test
    public void testGetToType()
    {
        assertEquals(coercer.getToType(), VarbinaryType.VARBINARY);
    }

    @Test
    public void testApply()
    {
        Block rowNumbers = new LongArrayBlockBuilder(null, 5)
                .writeLong(7L)
                .writeLong(Long.MIN_VALUE)
                .writeLong(0L)
                .writeLong(1L)
                .writeLong(-1L)
                .writeLong(Long.MAX_VALUE)
                .build();
        Block rowIDs = coercer.apply(rowNumbers);
        assertEquals(rowIDs.getPositionCount(), rowNumbers.getPositionCount());
        assertRowId(rowIDs, 0, 7L);
        assertRowId(rowIDs, 1, Long.MIN_VALUE);
        assertRowId(rowIDs, 2, 0L);
        assertRowId(rowIDs, 3, 1L);
        assertRowId(rowIDs, 4, -1L);
        assertRowId(rowIDs, 5, Long.MAX_VALUE);
    }

    private static void assertRowId(Block rowIDs, int position, long expected)
    {
        byte[] rowID = rowIDs.getSlice(position, 0, rowIDs.getSliceLength(position)).getBytes();
        assertEquals(rowID.length, 27);
        assertEquals(rowID[25], (byte) 8);
        assertEquals(rowID[26], (byte) 9);
        byte[] rowNumber = reverse(Arrays.copyOf(rowID, 8));
        assertEquals(Longs.fromByteArray(rowNumber), expected);
    }

    // I'm sure this can be micro-optimized. It's only a test. Clarity matters more.
    private static byte[] reverse(byte[] in)
    {
        byte[] out = new byte[in.length];
        for (int i = 0; i < in.length; i++) {
            out[i] = in[in.length - i - 1];
        }
        return out;
    }
}
