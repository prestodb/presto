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
package com.facebook.presto.client;

import com.facebook.presto.spi.session.ResourceEstimates;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

@Immutable
public class CreateQuerySession
{
    private final String catalog;
    private final String schema;
    private final String path;
    private final String user;
    private final String source;
    private final String userAgent;
    private final String timeZoneId;
    private final String language;
    private final String clientInfo;
    private final Set<String> clientTags;
    private final ResourceEstimates resourceEstimates;
    private final Map<String, String> properties;
    private final Map<String, String> preparedStatements;
    private final String traceToken;
    private final String transactionId;
    private final Set<String> clientCapabilities;

    @JsonCreator
    public CreateQuerySession(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("path") String path,
            @JsonProperty("user") String user,
            @JsonProperty("source") String source,
            @JsonProperty("userAgent") String userAgent,
            @JsonProperty("timeZoneId") String timeZoneId,
            @JsonProperty("language") String language,
            @JsonProperty("clientInfo") String clientInfo,
            @JsonProperty("clientTags") Set<String> clientTags,
            @JsonProperty("resourceEstimates") ResourceEstimates resourceEstimates,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements,
            @JsonProperty("traceToken") String traceToken,
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("clientCapabilities") Set<String> clientCapabilities)
    {
        checkArgument(catalog != null || schema == null, "schema is set but catalog is not");
        this.catalog = catalog;
        this.schema = schema;
        this.path = path;
        checkArgument(!isNullOrEmpty(user), "user is null or empty");
        this.user = user;
        this.source = source;
        this.userAgent = userAgent;
        this.timeZoneId = timeZoneId;
        this.language = language;
        this.clientInfo = clientInfo;
        this.clientTags = clientTags == null ? null : ImmutableSet.copyOf(clientTags);
        this.resourceEstimates = resourceEstimates;
        this.properties = properties == null ? null : ImmutableMap.copyOf(properties);
        this.preparedStatements = preparedStatements == null ? null : ImmutableMap.copyOf(preparedStatements);
        this.traceToken = traceToken;
        this.transactionId = transactionId;
        this.clientCapabilities = clientCapabilities == null ? null : ImmutableSet.copyOf(clientCapabilities);
    }

    @Nullable
    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @Nullable
    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @Nullable
    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @Nullable
    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @Nullable
    @JsonProperty
    public String getUserAgent()
    {
        return userAgent;
    }

    @Nullable
    @JsonProperty
    public String getClientInfo()
    {
        return clientInfo;
    }

    @Nullable
    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @Nullable
    @JsonProperty
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @Nullable
    @JsonProperty
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @Nullable
    @JsonProperty
    public String getLanguage()
    {
        return language;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    @Nullable
    @JsonProperty
    public String getTraceToken()
    {
        return traceToken;
    }

    @Nullable
    @JsonProperty
    public String getTransactionId()
    {
        return transactionId;
    }

    @Nullable
    @JsonProperty
    public Set<String> getClientCapabilities()
    {
        return clientCapabilities;
    }
}
