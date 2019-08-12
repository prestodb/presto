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
package com.facebook.presto.verifier.framework;

import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryConfiguration
{
    private final String catalog;
    private final String schema;
    private final String username;
    private final Optional<String> password;
    private final Map<String, String> sessionProperties;

    @JdbiConstructor
    public QueryConfiguration(
            @ColumnName("catalog") String catalog,
            @ColumnName("schema") String schema,
            @ColumnName("username") String username,
            @ColumnName("password") Optional<String> password,
            @ColumnName("session_properties") Optional<Map<String, String>> sessionProperties)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.username = requireNonNull(username, "username is null");
        this.password = requireNonNull(password, "password is null");
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties.orElse(ImmutableMap.of()));
    }

    public QueryConfiguration applyOverrides(QueryConfigurationOverrides overrides)
    {
        return new QueryConfiguration(
                overrides.getCatalogOverride().orElse(catalog),
                overrides.getSchemaOverride().orElse(schema),
                overrides.getUsernameOverride().orElse(username),
                Optional.ofNullable(overrides.getPasswordOverride().orElse(password.orElse(null))),
                Optional.of(sessionProperties));
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getUsername()
    {
        return username;
    }

    public Optional<String> getPassword()
    {
        return password;
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
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QueryConfiguration o = (QueryConfiguration) obj;
        return Objects.equals(catalog, o.catalog) &&
                Objects.equals(schema, o.schema) &&
                Objects.equals(username, o.username) &&
                Objects.equals(password, o.password) &&
                Objects.equals(sessionProperties, o.sessionProperties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema, username, password, sessionProperties);
    }
}
