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
package io.prestosql.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public final class CatalogSchemaTableName
{
    private final String catalogName;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public CatalogSchemaTableName(
            @JsonProperty("catalog") String catalogName,
            @JsonProperty("schemaTable") SchemaTableName schemaTableName)
    {
        this.catalogName = checkNotEmpty(catalogName, "catalogName");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    public CatalogSchemaTableName(String catalogName, String schemaName, String tableName)
    {
        this(catalogName, new SchemaTableName(schemaName, tableName));
    }

    @JsonProperty("catalog")
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty("schemaTable")
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

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaTableName.toString();
    }
}
