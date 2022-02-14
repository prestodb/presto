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

import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static org.testng.Assert.assertEquals;

public class TestStripeReader
{
    private static final long MILLION = 1_000_000;
    private static final long BILLION = 1000 * MILLION;
    private static final long TEN_BILLION = 10L * BILLION;

    @Test
    public void testCreateRowGroup()
    {
        long numRowsInStripe = TEN_BILLION + MILLION;
        long rowsInRowGroup = BILLION;

        for (int groupId = 0; groupId < 11; groupId++) {
            RowGroup rowGroup = StripeReader.createRowGroup(groupId, numRowsInStripe, rowsInRowGroup, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
            assertEquals(rowGroup.getGroupId(), groupId);
            assertEquals(rowGroup.getRowOffset(), groupId * (long) rowsInRowGroup);
            long expectedRows = groupId < 10 ? BILLION : MILLION;
            assertEquals(rowGroup.getRowCount(), expectedRows);
        }
    }

    @Test(expectedExceptions = ArithmeticException.class)
    public void testRowGroupOverflow()
    {
        StripeReader.createRowGroup(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testRowSize()
    {
        int numberOfEntries = 10_000;
        long numRowsInGroup = MILLION;
        IntegerStatistics integerStatistics = new IntegerStatistics(0L, 0L, 0L);
        ColumnStatistics intColumnStatistics = new IntegerColumnStatistics(numRowsInGroup, null, integerStatistics);
        ColumnStatistics mapColumnStatistics = new ColumnStatistics(numRowsInGroup, null);
        ColumnStatistics mapKeyColumnStatistics = new IntegerColumnStatistics(numRowsInGroup * numberOfEntries, null, integerStatistics);
        ColumnStatistics mapValueColumnStatistics = new IntegerColumnStatistics(numRowsInGroup * numberOfEntries, null, integerStatistics);

        StreamId intStreamId = new StreamId(1, 0, Stream.StreamKind.ROW_INDEX);
        StreamId mapStreamId = new StreamId(2, 0, Stream.StreamKind.ROW_INDEX);
        StreamId mapKeyStreamId = new StreamId(3, 0, Stream.StreamKind.ROW_INDEX);
        StreamId mapValueStreamId = new StreamId(4, 0, Stream.StreamKind.ROW_INDEX);

        Map<StreamId, List<RowGroupIndex>> columnIndexes = ImmutableMap.of(intStreamId, createRowGroupIndex(intColumnStatistics),
                mapStreamId, createRowGroupIndex(mapColumnStatistics),
                mapKeyStreamId, createRowGroupIndex(mapKeyColumnStatistics),
                mapValueStreamId, createRowGroupIndex(mapValueColumnStatistics));

        // Each row contains 1 integer, 2 * numberOfEntries * integer (2 is for key and value).
        long expectedRowSize = INTEGER_VALUE_BYTES + 2 * numberOfEntries * INTEGER_VALUE_BYTES;
        RowGroup rowGroup = StripeReader.createRowGroup(0, Long.MAX_VALUE, numRowsInGroup, columnIndexes, ImmutableMap.of(), ImmutableMap.of());
        assertEquals(expectedRowSize, rowGroup.getMinAverageRowBytes());
    }

    private static List<RowGroupIndex> createRowGroupIndex(ColumnStatistics columnStatistics)
    {
        return ImmutableList.of(new RowGroupIndex(ImmutableList.of(0, 0, 0, 0), columnStatistics));
    }
}
