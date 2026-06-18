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
package com.facebook.presto.spi.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public final class CatalogSchemaFunctionName
{
    private final String catalogName;
    private final SchemaFunctionName schemaFunctionName;

    public CatalogSchemaFunctionName(String catalogName, SchemaFunctionName schemaFunctionName)
    {
        this.catalogName = catalogName.toLowerCase(ROOT);
        if (catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName is empty");
        }
        this.schemaFunctionName = requireNonNull(schemaFunctionName, "schemaFunctionName is null");
    }

    @JsonCreator
    public CatalogSchemaFunctionName(
            @JsonProperty String catalogName,
            @JsonProperty String schemaName,
            @JsonProperty String functionName)
    {
        this(catalogName, new SchemaFunctionName(schemaName, functionName));
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    public SchemaFunctionName getSchemaFunctionName()
    {
        return schemaFunctionName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaFunctionName.getSchemaName();
    }

    @JsonProperty
    public String getFunctionName()
    {
        return schemaFunctionName.getFunctionName();
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
        CatalogSchemaFunctionName that = (CatalogSchemaFunctionName) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(schemaFunctionName, that.schemaFunctionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaFunctionName);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaFunctionName;
    }
}
