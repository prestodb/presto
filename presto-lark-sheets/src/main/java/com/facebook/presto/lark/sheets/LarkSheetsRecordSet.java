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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LarkSheetsRecordSet
        implements RecordSet
{
    private final LarkSheetsApi api;
    private final LarkSheetsSplit split;
    private final List<LarkSheetsColumnHandle> columns;
    private final List<Type> columnTypes;

    public LarkSheetsRecordSet(LarkSheetsApi api, LarkSheetsSplit split, List<LarkSheetsColumnHandle> columns)
    {
        this.api = requireNonNull(api, "api is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.columnTypes = columns.stream().map(LarkSheetsColumnHandle::getType).collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LarkSheetsRecordCursor(api, split, columns);
    }
}
