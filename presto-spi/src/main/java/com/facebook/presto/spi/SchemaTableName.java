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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;
import static java.util.Locale.ENGLISH;

public class SchemaTableName
{
    private final String schemaName;
    private final String tableName;
    private final String originalSchemaName;
    private final String originalTableName;

    @JsonCreator
    public SchemaTableName(@JsonProperty("schema") String schemaName, @JsonProperty("table") String tableName,
            @JsonProperty("originalSchema") String originalSchemaName, @JsonProperty("originalTable") String originalTableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName").toLowerCase(ENGLISH);
        this.tableName = checkNotEmpty(tableName, "tableName").toLowerCase(ENGLISH);
        this.originalSchemaName = checkNotEmpty(originalSchemaName, "actualSchemaName");
        this.originalTableName = checkNotEmpty(originalTableName, "actualTableName");
    }

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName").toLowerCase(ENGLISH);
        this.tableName = checkNotEmpty(tableName, "tableName").toLowerCase(ENGLISH);
        this.originalSchemaName = this.schemaName;
        this.originalTableName = this.tableName;
    }

    @JsonProperty("schema")
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty("table")
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty("originalSchema")
    public String getOriginalSchemaName()
    {
        return originalSchemaName;
    }

    @JsonProperty("originalTable")
    public String getOriginalTableName()
    {
        return originalTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, originalSchemaName, originalTableName);
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
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.originalSchemaName, other.originalSchemaName) &&
                Objects.equals(this.originalTableName, other.originalTableName);
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + tableName;
    }

    public SchemaTablePrefix toSchemaTablePrefix()
    {
        return new SchemaTablePrefix(schemaName, tableName, originalSchemaName, originalTableName);
    }
}
