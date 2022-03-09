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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class SheetsTable
{
    private final List<ColumnMetadata> columnsMetadata;
    private final List<List<Object>> values;

    @JsonCreator
    public SheetsTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<SheetsColumn> columns,
            @JsonProperty("values") List<List<Object>> values)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        requireNonNull(columns, "columns is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (SheetsColumn column : columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
        this.values = values;
    }

    @JsonProperty
    public List<List<Object>> getValues()
    {
        return values;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
