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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSTableHandle
    implements ConnectorTableHandle
{
    private final String clientId;
    private final String tableName;
    private final String schemaName;
    private String comment;
    private String location;
    private String owner;
    private StorageFormat storageFormat = StorageFormat.PARQUET;
    private List<HDFSColumnHandle> columns;
    private HDFSColumnHandle fiberColumn;
    private HDFSColumnHandle timestampColumn;
    private String fiberFunc;

    @JsonCreator
    public HDFSTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") String schemaName)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    @JsonCreator
    public HDFSTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("comment") String comment,
            @JsonProperty("location") String location,
            @JsonProperty("owner") String owner,
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("columns") List<HDFSColumnHandle> columns,
            @JsonProperty("fiberColumn") HDFSColumnHandle fiberColumn,
            @JsonProperty("timestampColumn") HDFSColumnHandle timestampColumn,
            @JsonProperty("fiberFunc") String fiberFunc)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.comment = requireNonNull(comment, "desc is null");
        this.location = requireNonNull(location, "location is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.fiberColumn = requireNonNull(fiberColumn, "fiberColumn is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.fiberFunc = requireNonNull(fiberFunc, "fiberFunc is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public String getDesc()
    {
        return comment;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }

    @JsonProperty
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    public List<HDFSColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public HDFSColumnHandle getFiberColumn()
    {
        return fiberColumn;
    }

    @JsonProperty
    public HDFSColumnHandle getTimestampColumn()
    {
        return timestampColumn;
    }

    @JsonProperty
    public String getFiberFunc()
    {
        return fiberFunc;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setStorageFormat(StorageFormat storageFormat)
    {
        this.storageFormat = storageFormat;
    }

    public void setColumns(List<HDFSColumnHandle> columns)
    {
        this.columns = columns;
    }

    public void setFiberColumn(HDFSColumnHandle fiberColumn)
    {
        this.fiberColumn = fiberColumn;
    }

    public void setTimestampColumn(HDFSColumnHandle timestampColumn)
    {
        this.timestampColumn = timestampColumn;
    }

    public void setFiberFunc(String fiberFunc)
    {
        this.fiberFunc = fiberFunc;
    }

    @Override
    public int hashCode()
    {
        // TODO hashCode
        return 1;
    }

    @Override
    public boolean equals(Object obj)
    {
        // TODO equals
        return true;
    }

    @Override
    public String toString()
    {
        // TODO toString
        return "";
    }
}
