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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;

public class SchemaTableName
{
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public static SchemaTableName valueOf(String schemaTableName)
    {
        checkNotEmpty(schemaTableName, "schemaTableName");
        String[] parts = schemaTableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid schemaTableName " + schemaTableName);
        }
        return new SchemaTableName(parts[0], parts[1]);
    }

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
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
        final SchemaTableName other = (SchemaTableName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return schemaName + '.' + tableName;
    }
}
