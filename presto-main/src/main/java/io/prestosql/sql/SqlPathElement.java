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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.Identifier;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SqlPathElement
{
    private final Optional<Identifier> catalog;
    private final Identifier schema;

    @JsonCreator
    public SqlPathElement(
            @JsonProperty("catalog") Optional<Identifier> catalog,
            @JsonProperty("schema") Identifier schema)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    @JsonProperty
    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Identifier getSchema()
    {
        return schema;
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
        SqlPathElement that = (SqlPathElement) obj;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema);
    }

    @Override
    public String toString()
    {
        if (catalog.isPresent()) {
            return format("%s.%s", catalog.get().toString(), schema.toString());
        }
        return schema.toString();
    }
}
