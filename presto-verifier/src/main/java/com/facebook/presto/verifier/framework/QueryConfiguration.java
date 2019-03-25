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

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryConfiguration
{
    private final String catalog;
    private final String schema;
    private final String username;
    private final Optional<String> password;
    private final Map<String, String> sessionProperties;

    public QueryConfiguration(
            String catalog,
            String schema,
            String username,
            Optional<String> password,
            Map<String, String> sessionProperties)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.username = requireNonNull(username, "username is null");
        this.password = requireNonNull(password, "password is null");
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
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
}
