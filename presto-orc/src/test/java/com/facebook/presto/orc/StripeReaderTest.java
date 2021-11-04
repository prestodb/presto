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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class StripeReaderTest
{
    private static final int MILLION = 1_000_000;
    private static final int BILLION = 1000 * MILLION;
    private static final long TEN_BILLION = 10L * BILLION;

    @Test
    public void testCreateRowGroup()
    {
        long numRowsInStripe = TEN_BILLION + MILLION;
        int rowsInRowGroup = BILLION;

        for (int groupId = 0; groupId < 11; groupId++) {
            RowGroup rowGroup = StripeReader.createRowGroup(groupId, numRowsInStripe, rowsInRowGroup, 5, ImmutableMap.of(), ImmutableMap.of());
            assertEquals(rowGroup.getGroupId(), groupId);
            assertEquals(rowGroup.getRowOffset(), groupId * (long) rowsInRowGroup);
            int expectedRows = groupId < 10 ? BILLION : MILLION;
            assertEquals(rowGroup.getRowCount(), expectedRows);
        }
    }

    @Test(expectedExceptions = ArithmeticException.class)
    public void testRowGroupOverflow()
    {
        StripeReader.createRowGroup(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 5, ImmutableMap.of(), ImmutableMap.of());
    }
}
