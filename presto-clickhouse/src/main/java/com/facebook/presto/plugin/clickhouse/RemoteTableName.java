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
package com.facebook.presto.plugin.clickhouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class RemoteTableName
{
    private final Optional<String> catalogName;
    private final Optional<String> schemaName;
    private final String tableName;

    @JsonCreator
    public RemoteTableName(Optional<String> catalogName, Optional<String> schemaName, String tableName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public Optional<String> getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
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
        RemoteTableName that = (RemoteTableName) o;
        return catalogName.equals(that.catalogName) &&
                schemaName.equals(that.schemaName) &&
                tableName.equals(that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(".").skipNulls().join(catalogName.orElse(null), schemaName.orElse(null), tableName);
    }
}
