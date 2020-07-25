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
package com.facebook.presto.spark.classloader_interface;

import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Based on com.facebook.presto.Session
 */
public class PrestoSparkSession
{
    private final String user;
    private final Optional<Principal> principal;
    private final Map<String, String> extraCredentials;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<String> source;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Optional<String> timeZoneId;
    private final Optional<String> language;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;
    private final Optional<String> traceToken;

    public PrestoSparkSession(
            String user,
            Optional<Principal> principal,
            Map<String, String> extraCredentials,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<String> source,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Optional<String> timeZoneId,
            Optional<String> language,
            Map<String, String> systemProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Optional<String> traceToken)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.extraCredentials = unmodifiableMap(new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null")));
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.source = requireNonNull(source, "source is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = unmodifiableSet(new HashSet<>(requireNonNull(clientTags, "clientTags is null")));
        this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId is null");
        this.language = requireNonNull(language, "language is null");
        this.systemProperties = unmodifiableMap(new HashMap<>(requireNonNull(systemProperties, "systemProperties is null")));
        this.catalogSessionProperties = unmodifiableMap(requireNonNull(catalogSessionProperties, "catalogSessionProperties is null").entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> unmodifiableMap(new HashMap<>(entry.getValue())))));
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
    }

    public String getUser()
    {
        return user;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public Optional<String> getTimeZoneId()
    {
        return timeZoneId;
    }

    public Optional<String> getLanguage()
    {
        return language;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }
}
