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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String tablePath;
    private final List<String> tableId;

    @JsonCreator
    public LanceTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("tableId") List<String> tableId)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.tableId = ImmutableList.copyOf(requireNonNull(tableId, "tableId is null"));
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
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public List<String> getTableId()
    {
        return tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tablePath);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        LanceTableHandle other = (LanceTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tablePath, other.tablePath);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("tablePath", tablePath)
                .toString();
    }
}
