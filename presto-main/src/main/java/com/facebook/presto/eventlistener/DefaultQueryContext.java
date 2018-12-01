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

package com.facebook.presto.eventlistener;

import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DefaultQueryContext
        implements QueryContext
{
    private final String user;
    private final Optional<String> principal;
    private final Optional<String> remoteClientAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final Optional<String> source;

    private final Optional<String> catalog;
    private final Optional<String> schema;

    private final Optional<ResourceGroupId> resourceGroupId;

    private final Map<String, String> sessionProperties;
    private final ResourceEstimates resourceEstimates;

    private final String serverAddress;
    private final String serverVersion;
    private final String environment;

    private final SessionPropertyManager sessionPropertyManager;

    public DefaultQueryContext(
            String user,
            Optional<String> principal,
            Optional<String> remoteClientAddress,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Set<String> clientCapabilities,
            Optional<String> source,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<ResourceGroupId> resourceGroupId,
            Map<String, String> sessionProperties,
            ResourceEstimates resourceEstimates,
            String serverAddress,
            String serverVersion,
            String environment,
            SessionPropertyManager sessionPropertyManager)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.remoteClientAddress = requireNonNull(remoteClientAddress, "remoteClientAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.clientCapabilities = requireNonNull(clientCapabilities, "clientCapabilities is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.serverAddress = requireNonNull(serverAddress, "serverAddress is null");
        this.serverVersion = requireNonNull(serverVersion, "serverVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @JsonProperty
    @Override
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    @Override
    public Optional<String> getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    @Override
    public Optional<String> getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    @JsonProperty
    @Override
    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    @Override
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @JsonProperty
    @Override
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    @Override
    public Set<String> getClientCapabilities()
    {
        return clientCapabilities;
    }

    @JsonProperty
    @Override
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    @Override
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    @Override
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    @Override
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    @Override
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    @JsonProperty
    @Override
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @JsonProperty
    @Override
    public String getServerAddress()
    {
        return serverAddress;
    }

    @JsonProperty
    @Override
    public String getServerVersion()
    {
        return serverVersion;
    }

    @JsonProperty
    @Override
    public String getEnvironment()
    {
        return environment;
    }

    @JsonIgnore
    @Override
    public <T> T getSystemProperty(String name, Class<T> type)
    {
        return sessionPropertyManager.decodeSystemPropertyValue(name, sessionProperties.get(name), type);
    }
}
