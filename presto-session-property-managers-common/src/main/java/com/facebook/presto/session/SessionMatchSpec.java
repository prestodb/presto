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
package com.facebook.presto.session;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SessionMatchSpec
{
    public static String emptyCatalog = "__NULL__";
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Set<String> clientTags;
    private final Optional<String> queryType;
    private final Optional<Pattern> clientInfoRegex;
    private final Optional<Pattern> resourceGroupRegex;
    private final Optional<Boolean> overrideSessionProperties;
    private final Map<String, String> sessionProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    @JsonCreator
    public SessionMatchSpec(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("clientTags") Optional<List<String>> clientTags,
            @JsonProperty("queryType") Optional<String> queryType,
            @JsonProperty("group") Optional<Pattern> resourceGroupRegex,
            @JsonProperty("clientInfo") Optional<Pattern> clientInfoRegex,
            @JsonProperty("overrideSessionProperties") Optional<Boolean> overrideSessionProperties,
            @JsonProperty("sessionProperties") Map<String, String> sessionProperties,
            @JsonProperty("catalogSessionProperties") Map<String, Map<String, String>> catalogSessionProperties)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        this.clientTags = ImmutableSet.copyOf(clientTags.orElse(ImmutableList.of()));
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.resourceGroupRegex = requireNonNull(resourceGroupRegex, "resourceGroupRegex is null");
        this.clientInfoRegex = requireNonNull(clientInfoRegex, "clientInfoRegex is null");
        this.overrideSessionProperties = requireNonNull(overrideSessionProperties, "overrideSessionProperties is null");

        checkArgument(sessionProperties != null || catalogSessionProperties != null,
                "Either sessionProperties or catalogSessionProperties must be provided");

        this.sessionProperties = ImmutableMap.copyOf(Optional.ofNullable(sessionProperties).orElse(ImmutableMap.of()));
        this.catalogSessionProperties = ImmutableMap.copyOf(Optional.ofNullable(catalogSessionProperties).orElse(ImmutableMap.of()));
    }

    public <T> Map<String, T> match(Map<String, T> object, SessionConfigurationContext context)
    {
        if (userRegex.isPresent() && !userRegex.get().matcher(context.getUser()).matches()) {
            return ImmutableMap.of();
        }
        if (sourceRegex.isPresent()) {
            String source = context.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return ImmutableMap.of();
            }
        }
        if (!clientTags.isEmpty() && !context.getClientTags().containsAll(clientTags)) {
            return ImmutableMap.of();
        }

        if (queryType.isPresent()) {
            String contextQueryType = context.getQueryType().orElse("");
            if (!queryType.get().equalsIgnoreCase(contextQueryType)) {
                return ImmutableMap.of();
            }
        }

        if (clientInfoRegex.isPresent()) {
            String clientInfo = context.getClientInfo().orElse("");
            if (!clientInfoRegex.get().matcher(clientInfo).matches()) {
                return ImmutableMap.of();
            }
        }

        if (resourceGroupRegex.isPresent()) {
            String resourceGroupId = context.getResourceGroupId().map(ResourceGroupId::toString).orElse("");
            if (!resourceGroupRegex.get().matcher(resourceGroupId).matches()) {
                return ImmutableMap.of();
            }
        }

        return object;
    }

    @JsonProperty("user")
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    @JsonProperty("source")
    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public Optional<String> getQueryType()
    {
        return queryType;
    }

    @JsonProperty("group")
    public Optional<Pattern> getResourceGroupRegex()
    {
        return resourceGroupRegex;
    }

    @JsonProperty
    public Optional<Pattern> getClientInfoRegex()
    {
        return clientInfoRegex;
    }

    @JsonProperty
    public Optional<Boolean> getOverrideSessionProperties()
    {
        return overrideSessionProperties;
    }

    @JsonProperty
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    @JsonProperty
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public static class Mapper
            implements RowMapper<SessionMatchSpec>
    {
        @Override
        public SessionMatchSpec map(ResultSet resultSet, StatementContext context)
                throws SQLException
        {
            SessionInfo sessionInfo = getProperties(
                    Optional.ofNullable(resultSet.getString("session_property_names")),
                    Optional.ofNullable(resultSet.getString("session_property_values")),
                    Optional.ofNullable(resultSet.getString("session_property_catalogs")));

            return new SessionMatchSpec(
                    Optional.ofNullable(resultSet.getString("user_regex")).map(Pattern::compile),
                    Optional.ofNullable(resultSet.getString("source_regex")).map(Pattern::compile),
                    Optional.ofNullable(resultSet.getString("client_tags")).map(tag -> Splitter.on(",").splitToList(tag)),
                    Optional.ofNullable(resultSet.getString("query_type")),
                    Optional.ofNullable(resultSet.getString("group_regex")).map(Pattern::compile),
                    Optional.ofNullable(resultSet.getString("client_info_regex")).map(Pattern::compile),
                    Optional.of(resultSet.getBoolean("override_session_properties")),
                    sessionInfo.getSessionProperties(),
                    sessionInfo.getCatalogProperties());
        }

        private SessionInfo getProperties(Optional<String> names, Optional<String> values, Optional<String> catalogs)
        {
            if (!names.isPresent()) {
                return new SessionInfo(ImmutableMap.of(), ImmutableMap.of());
            }

            checkArgument(catalogs.isPresent(), "names are present, but catalogs are not");
            checkArgument(values.isPresent(), "names are present, but values are not");
            List<String> sessionPropertyNames = Splitter.on(",").splitToList(names.get());
            List<String> sessionPropertyValues = Splitter.on(",").splitToList(values.get());
            List<String> sessionPropertyCatalogs = Splitter.on(",").splitToList(catalogs.get());
            checkArgument(sessionPropertyNames.size() == sessionPropertyValues.size(),
                    "The number of property names and values should be the same");

            checkArgument(sessionPropertyNames.size() == sessionPropertyCatalogs.size(),
                    "The number of property names and catalog values should be the same");

            Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
            Map<String, String> systemSessionProperties = new HashMap<>();
            for (int i = 0; i < sessionPropertyNames.size(); i++) {
                if (sessionPropertyCatalogs.get(i).equals(emptyCatalog)) {
                    systemSessionProperties.put(sessionPropertyNames.get(i), sessionPropertyValues.get(i));
                }
                else {
                    catalogSessionProperties
                            .computeIfAbsent(sessionPropertyCatalogs.get(i), k -> new HashMap<>())
                            .put(sessionPropertyNames.get(i), sessionPropertyValues.get(i));
                }
            }

            return new SessionInfo(systemSessionProperties, catalogSessionProperties);
        }
        static class SessionInfo
        {
            private final Map<String, String> sessionProperties;
            private final Map<String, Map<String, String>> catalogProperties;

            public SessionInfo(Map<String, String> sessionProperties,
                               Map<String, Map<String, String>> catalogProperties)
            {
                this.sessionProperties = sessionProperties;
                this.catalogProperties = catalogProperties;
            }

            public Map<String, String> getSessionProperties()
            {
                return sessionProperties;
            }

            public Map<String, Map<String, String>> getCatalogProperties()
            {
                return catalogProperties;
            }
        }
    }
}
