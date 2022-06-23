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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.NO_ACTION;

public class QueryConfigurationOverridesConfig
        implements QueryConfigurationOverrides
{
    private Optional<String> catalogOverride = Optional.empty();
    private Optional<String> schemaOverride = Optional.empty();
    private Optional<String> usernameOverride = Optional.empty();
    private Optional<String> passwordOverride = Optional.empty();
    private SessionPropertiesOverrideStrategy sessionPropertiesOverrideStrategy = NO_ACTION;
    private Map<String, String> sessionPropertiesOverride = ImmutableMap.of();
    private Set<String> sessionPropertiesToRemove = ImmutableSet.of();

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

    @Override
    @NotNull
    public SessionPropertiesOverrideStrategy getSessionPropertiesOverrideStrategy()
    {
        return sessionPropertiesOverrideStrategy;
    }

    @Config("session-properties-override-strategy")
    public QueryConfigurationOverridesConfig setSessionPropertiesOverrideStrategy(SessionPropertiesOverrideStrategy sessionPropertiesOverrideStrategy)
    {
        this.sessionPropertiesOverrideStrategy = sessionPropertiesOverrideStrategy;
        return this;
    }

    @Override
    public Set<String> getSessionPropertiesToRemove()
    {
        return sessionPropertiesToRemove;
    }

    @Config("session-properties-removal")
    public QueryConfigurationOverridesConfig setSessionPropertiesToRemove(String sessionPropertiesToRemove)
    {
        if (sessionPropertiesToRemove == null) {
            return this;
        }

        this.sessionPropertiesToRemove = ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(sessionPropertiesToRemove));
        return this;
    }

    @Override
    public Map<String, String> getSessionPropertiesOverride()
    {
        return sessionPropertiesOverride;
    }

    @Config("session-properties-override")
    public QueryConfigurationOverridesConfig setSessionPropertiesOverride(String sessionPropertyOverride)
    {
        if (sessionPropertyOverride == null) {
            return this;
        }

        try {
            this.sessionPropertiesOverride = new ObjectMapper().readValue(sessionPropertyOverride, new TypeReference<Map<String, String>>() {});
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}
