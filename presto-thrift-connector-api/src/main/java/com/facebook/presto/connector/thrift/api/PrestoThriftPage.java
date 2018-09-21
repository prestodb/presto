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
        this.rowCount = rowCount;
    }

    /**
     * Returns data in a columnar format.
     * Columns in this list are in the order given by the metadata of the insert table.
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
}
