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

public class CatalogSchemaTableName
{
    private final String catalogName;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public static CatalogSchemaTableName valueOf(String catalogSchemaTableName)
    {
        checkNotEmpty(catalogSchemaTableName, "catalogSchemaTableName");
        String[] parts = catalogSchemaTableName.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid catalogSchemaTableName " + catalogSchemaTableName);
        }
        return new CatalogSchemaTableName(parts[0], parts[1], parts[2]);
    }

    public CatalogSchemaTableName(String catalogName, SchemaTableName schemaTableName)
    {
        this.catalogName = catalogName;
        this.schemaTableName = schemaTableName;
    }

    public CatalogSchemaTableName(String catalogName, String schemaName, String tableName)
    {
        this.catalogName = catalogName;
        this.schemaTableName = new SchemaTableName(schemaName, tableName);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogSchemaTableName that = (CatalogSchemaTableName) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaTableName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaTableName.toString();
    }
}
