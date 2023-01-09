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
package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PrestoSparkRunnerContext
{
    private final String user;
    private final Optional<Principal> principal;
    private final Map<String, String> extraCredentials;
    private final String catalog;
    private final String schema;
    private final Optional<String> source;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Map<String, String> sessionProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;
    private final Optional<String> sqlText;
    private final Optional<String> sqlLocation;
    private final Optional<String> sqlFileHexHash;
    private final Optional<String> sqlFileSizeInBytes;
    private final Optional<String> traceToken;
    private final Optional<String> sparkQueueName;
    private final Optional<String> queryStatusInfoOutputLocation;
    private final Optional<String> queryDataOutputLocation;
    private final Optional<RetryExecutionStrategy> retryExecutionStrategy;

    public PrestoSparkRunnerContext(
            String user,
            Optional<Principal> principal,
            Map<String, String> extraCredentials,
            String catalog,
            String schema,
            Optional<String> source,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Map<String, String> sessionProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Optional<String> sqlText,
            Optional<String> sqlLocation,
            Optional<String> sqlFileHexHash,
            Optional<String> sqlFileSizeInBytes,
            Optional<String> traceToken,
            Optional<String> sparkQueueName,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation,
            Optional<RetryExecutionStrategy> retryExecutionStrategy)
    {
        this.user = user;
        this.principal = principal;
        this.extraCredentials = extraCredentials;
        this.catalog = catalog;
        this.schema = schema;
        this.source = source;
        this.userAgent = userAgent;
        this.clientInfo = clientInfo;
        this.clientTags = clientTags;
        this.sessionProperties = sessionProperties;
        this.catalogSessionProperties = catalogSessionProperties;
        this.sqlText = sqlText;
        this.sqlLocation = sqlLocation;
        this.sqlFileHexHash = sqlFileHexHash;
        this.sqlFileSizeInBytes = sqlFileSizeInBytes;
        this.traceToken = traceToken;
        this.sparkQueueName = sparkQueueName;
        this.queryStatusInfoOutputLocation = queryStatusInfoOutputLocation;
        this.queryDataOutputLocation = queryDataOutputLocation;
        this.retryExecutionStrategy = retryExecutionStrategy;
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

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
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

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public Optional<String> getSqlText()
    {
        return sqlText;
    }

    public Optional<String> getSqlLocation()
    {
        return sqlLocation;
    }

    public Optional<String> getSqlFileHexHash()
    {
        return sqlFileHexHash;
    }

    public Optional<String> getSqlFileSizeInBytes()
    {
        return sqlFileSizeInBytes;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public Optional<String> getSparkQueueName()
    {
        return sparkQueueName;
    }

    public Optional<String> getQueryStatusInfoOutputLocation()
    {
        return queryStatusInfoOutputLocation;
    }

    public Optional<String> getQueryDataOutputLocation()
    {
        return queryDataOutputLocation;
    }

    public Optional<RetryExecutionStrategy> getRetryExecutionStrategy()
    {
        return retryExecutionStrategy;
    }

    public static class Builder
    {
        private String user;
        private Optional<Principal> principal;
        private Map<String, String> extraCredentials;
        private String catalog;
        private String schema;
        private Optional<String> source;
        private Optional<String> userAgent;
        private Optional<String> clientInfo;
        private Set<String> clientTags;
        private Map<String, String> sessionProperties;
        private Map<String, Map<String, String>> catalogSessionProperties;
        private Optional<String> sqlText;
        private Optional<String> sqlLocation;
        private Optional<String> sqlFileHexHash;
        private Optional<String> sqlFileSizeInBytes;
        private Optional<String> traceToken;
        private Optional<String> sparkQueueName;
        private Optional<String> queryStatusInfoOutputLocation;
        private Optional<String> queryDataOutputLocation;
        private Optional<RetryExecutionStrategy> retryExecutionStrategy;

        public Builder(PrestoSparkRunnerContext prestoSparkRunnerContext)
        {
            this.user = prestoSparkRunnerContext.getUser();
            this.principal = prestoSparkRunnerContext.getPrincipal();
            this.extraCredentials = prestoSparkRunnerContext.getExtraCredentials();
            this.catalog = prestoSparkRunnerContext.getCatalog();
            this.schema = prestoSparkRunnerContext.getSchema();
            this.source = prestoSparkRunnerContext.getSource();
            this.userAgent = prestoSparkRunnerContext.getUserAgent();
            this.clientInfo = prestoSparkRunnerContext.getClientInfo();
            this.clientTags = prestoSparkRunnerContext.getClientTags();
            this.sessionProperties = prestoSparkRunnerContext.getSessionProperties();
            this.catalogSessionProperties = prestoSparkRunnerContext.getCatalogSessionProperties();
            this.sqlText = prestoSparkRunnerContext.getSqlText();
            this.sqlLocation = prestoSparkRunnerContext.getSqlLocation();
            this.sqlFileHexHash = prestoSparkRunnerContext.getSqlFileHexHash();
            this.sqlFileSizeInBytes = prestoSparkRunnerContext.getSqlFileSizeInBytes();
            this.traceToken = prestoSparkRunnerContext.getTraceToken();
            this.sparkQueueName = prestoSparkRunnerContext.getSparkQueueName();
            this.queryStatusInfoOutputLocation = prestoSparkRunnerContext.getQueryStatusInfoOutputLocation();
            this.queryDataOutputLocation = prestoSparkRunnerContext.getQueryDataOutputLocation();
            this.retryExecutionStrategy = prestoSparkRunnerContext.getRetryExecutionStrategy();
        }

        public Builder setRetryExecutionStrategy(Optional<RetryExecutionStrategy> retryExecutionStrategy)
        {
            this.retryExecutionStrategy = requireNonNull(retryExecutionStrategy, "retryExecutionStrategy is null");
            return this;
        }

        public PrestoSparkRunnerContext build()
        {
            return new PrestoSparkRunnerContext(
                    user,
                    principal,
                    extraCredentials,
                    catalog,
                    schema,
                    source,
                    userAgent,
                    clientInfo,
                    clientTags,
                    sessionProperties,
                    catalogSessionProperties,
                    sqlText,
                    sqlLocation,
                    sqlFileHexHash,
                    sqlFileSizeInBytes,
                    traceToken,
                    sparkQueueName,
                    queryStatusInfoOutputLocation,
                    queryDataOutputLocation,
                    retryExecutionStrategy);
        }
    }
}
