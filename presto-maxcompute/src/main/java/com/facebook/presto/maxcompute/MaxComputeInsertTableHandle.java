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
package com.facebook.presto.maxcompute;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MaxComputeInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final List<MaxComputeColumnHandle> columnHandleList;
    private final List<Type> typeList;

    @JsonCreator
    public MaxComputeInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandleList") List<MaxComputeColumnHandle> columnHandleList,
            @JsonProperty("typeList") List<Type> typeList)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnHandleList = requireNonNull(columnHandleList, "columnHandleList is null");
        this.typeList = requireNonNull(typeList, "typeList is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
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
    public List<Type> getTypeList()
    {
        return typeList;
    }

    @JsonProperty
    public List<MaxComputeColumnHandle> getColumnHandleList()
    {
        return columnHandleList;
    }

    @Override
    public String toString()
    {
        return format("odps:%s.%s", schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                schemaName,
                tableName,
                columnHandleList,
                typeList);
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
        MaxComputeInsertTableHandle other = (MaxComputeInsertTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columnHandleList, other.columnHandleList) &&
                Objects.equals(this.typeList, other.typeList);
    }
}
