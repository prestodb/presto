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
package com.facebook.presto.benchmark.framework;

import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BenchmarkQuery
{
    private final String name;
    private final String query;
    private final String catalog;
    private final String schema;
    private final Map<String, String> sessionProperties;

    @JdbiConstructor
    public BenchmarkQuery(
            @ColumnName("name") String name,
            @ColumnName("query") String query,
            @ColumnName("catalog") String catalog,
            @ColumnName("schema") String schema,
            @ColumnName("session_properties") Optional<Map<String, String>> sessionProperties)
    {
        this.name = requireNonNull(name, "name is null");
        this.query = clean(query);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.sessionProperties = sessionProperties.orElse(ImmutableMap.of());
    }

    public String getName()
    {
        return name;
    }

    public String getQuery()
    {
        return query;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
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
        BenchmarkQuery o = (BenchmarkQuery) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(query, o.query) &&
                Objects.equals(catalog, o.catalog) &&
                Objects.equals(schema, o.schema) &&
                Objects.equals(sessionProperties, o.sessionProperties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, catalog, schema, sessionProperties);
    }

    private static String clean(String sql)
    {
        sql = sql.replaceAll("\t", "  ");
        sql = sql.replaceAll("\n+", "\n");
        sql = sql.trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
