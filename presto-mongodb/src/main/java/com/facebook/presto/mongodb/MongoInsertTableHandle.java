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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MongoInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final MongoTableHandle table;
    private final List<MongoColumnHandle> columns;

    @JsonCreator
    public MongoInsertTableHandle(
            @JsonProperty("table") MongoTableHandle table,
            @JsonProperty("columns") List<MongoColumnHandle> columns)
    {
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public MongoTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<MongoColumnHandle> getColumns()
    {
        return columns;
    }
}
