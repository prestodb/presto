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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableFunctionApplicationResult<T>
{
    private final T tableHandle;
    private final List<ColumnHandle> columnHandles;

    public TableFunctionApplicationResult(T tableHandle, List<ColumnHandle> columnHandles)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    public T getTableHandle()
    {
        return tableHandle;
    }

    public List<ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
}
