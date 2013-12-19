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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.OutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

public class NativeOutputTableHandle
        implements OutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<NativeColumnHandle> columnHandles;
    private final List<ColumnType> columnTypes;

    @JsonCreator
    public NativeOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<NativeColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<ColumnType> columnTypes)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
        this.columnHandles = ImmutableList.copyOf(checkNotNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<NativeColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public String toString()
    {
        return "native:" + schemaName + "." + tableName;
    }
}
