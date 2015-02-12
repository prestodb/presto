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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

public class SystemTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public SystemTableHandle(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
    }

    public SystemTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
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

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return "system:" + schemaName + "." + tableName;
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
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SystemTableHandle other = (SystemTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }
}
