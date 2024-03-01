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
package com.facebook.presto.google.sheets;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SheetsTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public SheetsTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        this.schemaTableName = new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaTableName.getSchemaName();
    }

    @JsonProperty
    public String getTableName()
    {
        return schemaTableName.getTableName();
    }

    public SchemaTableName toSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
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

        SheetsTableHandle other = (SheetsTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
