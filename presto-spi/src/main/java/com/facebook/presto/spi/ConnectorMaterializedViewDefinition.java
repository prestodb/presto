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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class ConnectorMaterializedViewDefinition
{
    private final String originalSql;
    private final Optional<String> catalog;
    private final String schema;
    private final String table;
    private final List<ViewBaseTable> baseTables;
    private final Optional<String> owner;
    // A 1-to-1 map from view column name to a tuple of base table name + base table column name
    @JsonIgnore
    private Optional<Map<String, Map<SchemaTableName, String>>> viewToBaseColumnMap;

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("baseTables") List<ViewBaseTable> baseTables,
            @JsonProperty("owner") Optional<String> owner)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.baseTables = unmodifiableList(new ArrayList<>(requireNonNull(baseTables, "baseTables is null")));
        this.owner = requireNonNull(owner, "owner is null");
        this.viewToBaseColumnMap = Optional.empty(); }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public List<ViewBaseTable> getBaseTables()
    {
        return baseTables;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonIgnore
    public Optional<Map<String, Map<SchemaTableName, String>>> getViewToBaseColumnMap()
    {
        return viewToBaseColumnMap;
    }

    @JsonIgnore
    public void setViewToBaseColumnMap(Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        this.viewToBaseColumnMap = Optional.of(unmodifiableMap(new HashMap<>(requireNonNull(viewToBaseColumnMap, "viewToBaseColumnMap is null"))));
    }


    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorMaterializedViewDefinition{");
        sb.append("originalSql=").append(originalSql);
        sb.append(",catalog=").append(catalog.orElse(null));
        sb.append(",schema=").append(schema);
        sb.append(",table=").append(table);
        sb.append(",baseTables=").append(baseTables);
        sb.append(",owner=").append(owner.orElse(null));
        sb.append("}");
        return sb.toString();
    }

    public static final class ViewBaseTable
    {
        private final SchemaTableName name;

        @JsonCreator
        public ViewBaseTable(
                @JsonProperty("name") SchemaTableName name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @JsonProperty
        public SchemaTableName getName()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return name.toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
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

            ViewBaseTable baseTable = (ViewBaseTable) o;
            return Objects.equals(name, baseTable.name);
        }
    }
}
