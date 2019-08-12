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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class QueryConfigurationOverridesConfig
        implements QueryConfigurationOverrides
{
    private Optional<String> catalogOverride = Optional.empty();
    private Optional<String> schemaOverride = Optional.empty();
    private Optional<String> usernameOverride = Optional.empty();
    private Optional<String> passwordOverride = Optional.empty();

    @Override
    public Optional<String> getCatalogOverride()
    {
        return catalogOverride;
    }

    @ConfigDescription("Overrides the catalog for all the queries")
    @Config("catalog-override")
    public QueryConfigurationOverridesConfig setCatalogOverride(String catalogOverride)
    {
        this.catalogOverride = Optional.ofNullable(catalogOverride);
        return this;
    }

    @Override
    public Optional<String> getSchemaOverride()
    {
        return schemaOverride;
    }

    @ConfigDescription("Overrides the schema for all the queries")
    @Config("schema-override")
    public QueryConfigurationOverridesConfig setSchemaOverride(String schemaOverride)
    {
        this.schemaOverride = Optional.ofNullable(schemaOverride);
        return this;
    }

    @Override
    public Optional<String> getUsernameOverride()
    {
        return usernameOverride;
    }

    @ConfigDescription("Overrides the username for all the queries")
    @Config("username-override")
    public QueryConfigurationOverridesConfig setUsernameOverride(String usernameOverride)
    {
        this.usernameOverride = Optional.ofNullable(usernameOverride);
        return this;
    }

    @Override
    public Optional<String> getPasswordOverride()
    {
        return passwordOverride;
    }

    @ConfigDescription("Overrides the password for all the queries")
    @Config("password-override")
    public QueryConfigurationOverridesConfig setPasswordOverride(String passwordOverride)
    {
        this.passwordOverride = Optional.ofNullable(passwordOverride);
        return this;
    }
}
