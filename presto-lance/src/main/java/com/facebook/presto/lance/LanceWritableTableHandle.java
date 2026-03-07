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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceWritableTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String schemaJson;
    private final List<LanceColumnHandle> inputColumns;

    @JsonCreator
    public LanceWritableTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaJson") String schemaJson,
            @JsonProperty("inputColumns") List<LanceColumnHandle> inputColumns)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
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
    public String getSchemaJson()
    {
        return schemaJson;
    }

    @JsonProperty
    public List<LanceColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
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
        LanceWritableTableHandle other = (LanceWritableTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }
}
