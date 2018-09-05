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
package com.facebook.presto.connector.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftPage
{
    private final List<PrestoThriftBlock> columnBlocks;
    private final int rowCount;

    @ThriftConstructor
    public PrestoThriftPage(List<PrestoThriftBlock> columnBlocks, int rowCount)
    {
        this.columnBlocks = requireNonNull(columnBlocks, "columnBlocks is null");
        checkArgument(rowCount >= 0, "rowCount is negative");
        // checkAllColumnsAreOfExpectedSize(columnBlocks, rowCount);
        this.rowCount = rowCount;
    }

    /**
     * Returns data in a columnar format.
     * Columns in this list must be in the order they were requested by the engine.
     */
    @ThriftField(1)
    public List<PrestoThriftBlock> getColumnBlocks()
    {
        return columnBlocks;
    }

    @ThriftField(2)
    public int getRowCount()
    {
        return rowCount;
    }

    /*@Nullable
    public Page toPage(List<Type> columnTypes)
    {
        if (rowCount == 0) {
            return null;
        }
        checkArgument(columnBlocks.size() == columnTypes.size(), "columns and types have different sizes");
        int numberOfColumns = columnBlocks.size();
        if (numberOfColumns == 0) {
            // request/response with no columns, used for queries like "select count star"
            return new Page(rowCount);
        }
        Block[] blocks = new Block[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            blocks[i] = columnBlocks.get(i).toBlock(columnTypes.get(i));
        }
        return new Page(blocks);
    }*/

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftPage other = (PrestoThriftPage) obj;
        return Objects.equals(this.columnBlocks, other.columnBlocks) &&
                this.rowCount == other.rowCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnBlocks, rowCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnBlocks", columnBlocks)
                .add("rowCount", rowCount)
                .toString();
    }

    /*public static PrestoThriftPageResult fromRecordSet(RecordSet recordSet)
    {
        List<Type> types = recordSet.getColumnTypes();
        int numberOfColumns = types.size();
        int positions = totalRecords(recordSet);
        if (numberOfColumns == 0) {
            return new PrestoThriftPageResult(ImmutableList.of(), positions, null);
        }
        List<PrestoThriftBlock> thriftBlocks = new ArrayList<>(numberOfColumns);
        for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
            thriftBlocks.add(fromRecordSetColumn(recordSet, columnIndex, positions));
        }
        return new PrestoThriftPageResult(thriftBlocks, positions, null);
    }

    private static void checkAllColumnsAreOfExpectedSize(List<PrestoThriftBlock> columnBlocks, int expectedNumberOfRows)
    {
        for (int i = 0; i < columnBlocks.size(); i++) {
            checkArgument(columnBlocks.get(i).numberOfRecords() == expectedNumberOfRows,
                    "Incorrect number of records for column with index %s: expected %s, got %s",
                    i, expectedNumberOfRows, columnBlocks.get(i).numberOfRecords());
        }
    }

    private static int totalRecords(RecordSet recordSet)
    {
        RecordCursor cursor = recordSet.cursor();
        int result = 0;
        while (cursor.advanceNextPosition()) {
            result++;
        }
        return result;
    }*/
}
