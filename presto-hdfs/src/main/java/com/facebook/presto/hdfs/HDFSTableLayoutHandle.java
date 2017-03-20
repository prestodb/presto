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

import com.facebook.presto.hdfs.function.Function;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSTableLayoutHandle
implements ConnectorTableLayoutHandle
{
    private final HDFSTableHandle table;
    private final HDFSColumnHandle fiberColumn;
    private final HDFSColumnHandle timestampColumn;
    private final Function fiberFunction;
    private final StorageFormat storageFormat;

    @JsonCreator
    public HDFSTableLayoutHandle(
            @JsonProperty("table") HDFSTableHandle table)
    {
        this.table = table;
        this.fiberColumn = null;
        this.timestampColumn = null;
        this.fiberFunction = null;
        this.storageFormat = StorageFormat.PARQUET;
    }

    @JsonCreator
    public HDFSTableLayoutHandle(
            @JsonProperty("table") HDFSTableHandle table,
            @JsonProperty("fiberColumn") HDFSColumnHandle fiberColumn,
            @JsonProperty("timestampColumn") HDFSColumnHandle timestampColumn,
            @JsonProperty("fiberFunction") Function fiberFunction,
            @JsonProperty("storageFormat") StorageFormat storageFormat)
    {
        this.table = requireNonNull(table, "tableName is null");
        this.fiberColumn = requireNonNull(fiberColumn, "fiberColumn is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.fiberFunction = requireNonNull(fiberFunction, "fiberFunc is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return new SchemaTableName(table.getSchemaName(), table.getTableName());
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
    public Function getFiberFunc()
    {
        return fiberFunction;
    }

    @JsonProperty
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    // TODO Override toString(), hashCode(), and equals()
}
