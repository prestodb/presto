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
package com.facebook.presto.raptorx.transaction;

import com.facebook.presto.raptorx.metadata.ColumnInfo;

import static java.util.Objects.requireNonNull;

public class AddColumnAction
        implements Action
{
    private final long tableId;
    private final ColumnInfo columnInfo;

    public AddColumnAction(long tableId, ColumnInfo columnInfo)
    {
        this.tableId = tableId;
        this.columnInfo = requireNonNull(columnInfo, "columnInfo is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public ColumnInfo getColumnInfo()
    {
        return columnInfo;
    }

    @Override
    public void accept(ActionVisitor visitor)
    {
        visitor.visitAddColumn(this);
    }
}
