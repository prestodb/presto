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
package com.facebook.presto.google.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SheetsRecordSet
        implements RecordSet
{
    private final List<SheetsColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<List<Object>> values;

    public SheetsRecordSet(SheetsSplit split, List<SheetsColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.values = split.getValues();
        this.columnTypes = columnHandles.stream().map(SheetsColumnHandle::getColumnType).collect(Collectors.toList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new SheetsRecordCursor(columnHandles, values);
    }
}
