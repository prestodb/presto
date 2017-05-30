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

import static java.util.Objects.requireNonNull;

public class RenameColumnAction
        implements Action
{
    private final long tableId;
    private final long columnId;
    private final String columnName;

    public RenameColumnAction(long tableId, long columnId, String columnName)
    {
        this.tableId = tableId;
        this.columnId = columnId;
        this.columnName = requireNonNull(columnName, "tableName is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getColumnId()
    {
        return columnId;
    }

    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public void accept(ActionVisitor visitor)
    {
        visitor.visitRenameColumn(this);
    }
}
