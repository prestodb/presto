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
package com.facebook.presto.lance;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestRowAddress
{
    @Test
    public void testEncodeZero()
    {
        assertEquals(RowAddress.encode(0, 0), 0L);
    }

    @Test
    public void testEncodeKnownLayout()
    {
        // Row address layout: (fragmentId << 32) | rowIndex
        assertEquals(RowAddress.encode(3, 7), (3L << 32) | 7L);
    }

    @Test
    public void testDecodeFragmentId()
    {
        assertEquals(RowAddress.fragmentId(RowAddress.encode(42, 1000)), 42);
    }

    @Test
    public void testDecodeRowIndex()
    {
        assertEquals(RowAddress.rowIndex(RowAddress.encode(42, 1000)), 1000);
    }

    @Test
    public void testRoundTrip()
    {
        int[] fragmentIds = {0, 1, 5, 1024, Integer.MAX_VALUE};
        int[] rowIndexes = {0, 1, 7, 1_000_000, Integer.MAX_VALUE};
        for (int fragmentId : fragmentIds) {
            for (int rowIndex : rowIndexes) {
                long encoded = RowAddress.encode(fragmentId, rowIndex);
                assertEquals(RowAddress.fragmentId(encoded), fragmentId);
                assertEquals(RowAddress.rowIndex(encoded), rowIndex);
            }
        }
    }
}
