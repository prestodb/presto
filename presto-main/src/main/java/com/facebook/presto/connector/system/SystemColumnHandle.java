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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

public class SystemColumnHandle
        implements ConnectorColumnHandle
{
    private final String columnName;

    @JsonCreator
    public SystemColumnHandle(@JsonProperty("columnName") String columnName)
    {
        this.columnName = columnName;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
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
        final SystemColumnHandle other = (SystemColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return "system:" + columnName;
    }

    public static Map<String, ConnectorColumnHandle> toSystemColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            columnHandles.put(columnMetadata.getName(), new SystemColumnHandle(columnMetadata.getName()));
        }

        return columnHandles.build();
    }
}
