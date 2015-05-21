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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public final class BlackHoleTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<BlackHoleColumnHandle> columnHandles;

    public BlackHoleTableHandle(ConnectorTableMetadata tableMetadata)
    {
        schemaName = tableMetadata.getTable().getSchemaName();
        tableName = tableMetadata.getTable().getTableName();
        columnHandles = tableMetadata.getColumns().stream()
                .map(BlackHoleColumnHandle::new)
                .collect(toList());
    }

    @JsonCreator
    public BlackHoleTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<BlackHoleColumnHandle> columnHandles)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnHandles = columnHandles;
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
    public List<BlackHoleColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    public ConnectorTableMetadata toTableMetadata(TypeManager typeManager)
    {
        return new ConnectorTableMetadata(
                toSchemaTableName(),
                columnHandles.stream().map(columnHandle -> columnHandle.toColumnMetadata(typeManager)).collect(toList()));
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSchemaName(), getTableName());
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
        BlackHoleTableHandle other = (BlackHoleTableHandle) obj;
        return Objects.equals(this.getSchemaName(), other.getSchemaName()) &&
                Objects.equals(this.getTableName(), other.getTableName());
    }
}
