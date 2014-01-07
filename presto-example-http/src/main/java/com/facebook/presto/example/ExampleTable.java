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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ExampleTable
{
    private final String name;
    private final List<ExampleColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<URI> sources;

    @JsonCreator
    public ExampleTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ExampleColumn> columns,
            @JsonProperty("sources") List<URI> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(checkNotNull(sources, "sources is null"));

        int index = 0;
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (ExampleColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(), index, false));
            index++;
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ExampleColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<URI> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public static Function<ExampleTable, String> nameGetter()
    {
        return new Function<ExampleTable, String>()
        {
            @Override
            public String apply(ExampleTable table)
            {
                return table.getName();
            }
        };
    }
}
