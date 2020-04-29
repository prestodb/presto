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
package com.facebook.presto.common.function;

import com.facebook.presto.common.CatalogSchemaName;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CatalogSchemaPrefix
{
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    private final String catalogName;
    private final Optional<String> schemaName;

    public CatalogSchemaPrefix(String catalogName, Optional<String> schemaName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public static CatalogSchemaPrefix of(String prefix)
    {
        String[] parts = prefix.split("\\.");
        if (parts.length != 1 && parts.length != 2) {
            throw new IllegalArgumentException("CatalogSchemaPrefix should have 1 or 2 parts");
        }
        return parts.length == 1 ? new CatalogSchemaPrefix(parts[0], Optional.empty()) : new CatalogSchemaPrefix(parts[0], Optional.of(parts[1]));
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public boolean includes(CatalogSchemaName catalogSchemaName)
    {
        return catalogName.equals(catalogSchemaName.getCatalogName())
                && (!schemaName.isPresent() || schemaName.get().equals(catalogSchemaName.getSchemaName()));
    }

    public boolean includes(CatalogSchemaPrefix that)
    {
        return catalogName.equals(that.getCatalogName())
                && (!schemaName.isPresent() || (that.schemaName.isPresent() && schemaName.get().equals(that.schemaName.get())));
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
        CatalogSchemaPrefix that = (CatalogSchemaPrefix) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(schemaName, that.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName);
    }

    @Override
    public String toString()
    {
        return catalogName + (schemaName.map(name -> "." + name).orElse(""));
    }
}
