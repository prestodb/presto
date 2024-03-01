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
package com.facebook.presto.verifier.source;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class PrestoQuerySourceQueryConfig
{
    private String query;
    private String catalog;
    private String schema;
    private Optional<String> username = Optional.empty();
    private Optional<String> password = Optional.empty();

    @NotNull
    public String getQuery()
    {
        return query;
    }

    @Config("query")
    public PrestoQuerySourceQueryConfig setQuery(String query)
    {
        this.query = query;
        return this;
    }

    @NotNull
    public String getCatalog()
    {
        return catalog;
    }

    @Config("catalog")
    public PrestoQuerySourceQueryConfig setCatalog(String catalog)
    {
        this.catalog = catalog;
        return this;
    }

    @NotNull
    public String getSchema()
    {
        return schema;
    }

    @Config("schema")
    public PrestoQuerySourceQueryConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    @NotNull
    public Optional<String> getUsername()
    {
        return username;
    }

    @Config("username")
    public PrestoQuerySourceQueryConfig setUsername(String username)
    {
        this.username = Optional.ofNullable(username);
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("password")
    public PrestoQuerySourceQueryConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }
}
