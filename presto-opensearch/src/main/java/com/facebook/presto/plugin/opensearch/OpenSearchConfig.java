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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

/**
 * Configuration for OpenSearch connector.
 * Provides connection, authentication, query settings, and nested field support.
 */
public class OpenSearchConfig
{
    private String host = "localhost";
    private int port = 9200;
    private String scheme = "https";
    private String username;
    private String password;
    private boolean sslEnabled = true;
    private boolean sslVerifyHostname = true;
    private boolean sslSkipCertificateValidation;
    private String sslTruststorePath;
    private String sslTruststorePassword;
    private boolean vectorSearchEnabled = true;
    private int vectorSearchDefaultK = 10;
    private String vectorSearchDefaultSpaceType = "cosine";
    private int vectorSearchDefaultEfSearch = 100;
    private int scrollSize = 1000;
    private String scrollTimeout = "5m";
    private int maxConnections = 100;
    private int connectionTimeout = 10000;
    private int socketTimeout = 60000;
    private String schemaPattern = "*";
    private boolean hideSystemIndices = true;
    private int maxResultWindow = 10000;

    // Nested field configuration
    private boolean nestedFieldAccessEnabled = true;
    private int nestedMaxDepth = 5;
    private boolean nestedOptimizeQueries = true;
    private boolean nestedDiscoverDynamicFields;
    private boolean nestedLogMissingFields;

    // Column visibility configuration
    private boolean hideDocumentIdColumn;

    public String getHost()
    {
        return host;
    }

    @Config("opensearch.host")
    @ConfigDescription("OpenSearch host")
    public OpenSearchConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("opensearch.port")
    @ConfigDescription("OpenSearch port")
    public OpenSearchConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    public String getScheme()
    {
        return scheme;
    }

    @Config("opensearch.scheme")
    @ConfigDescription("Connection scheme (http or https)")
    public OpenSearchConfig setScheme(String scheme)
    {
        this.scheme = scheme;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("opensearch.auth.username")
    @ConfigDescription("Authentication username")
    public OpenSearchConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("opensearch.auth.password")
    @ConfigDescription("Authentication password")
    @ConfigSecuritySensitive
    public OpenSearchConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("opensearch.ssl.enabled")
    @ConfigDescription("Enable SSL/TLS")
    public OpenSearchConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public boolean isSslVerifyHostname()
    {
        return sslVerifyHostname;
    }

    @Config("opensearch.ssl.verify-hostname")
    @ConfigDescription("Verify SSL hostname")
    public OpenSearchConfig setSslVerifyHostname(boolean sslVerifyHostname)
    {
        this.sslVerifyHostname = sslVerifyHostname;
        return this;
    }

    public String getSslTruststorePath()
    {
        return sslTruststorePath;
    }

    @Config("opensearch.ssl.truststore.path")
    @ConfigDescription("Path to SSL truststore")
    public OpenSearchConfig setSslTruststorePath(String sslTruststorePath)
    {
        this.sslTruststorePath = sslTruststorePath;
        return this;
    }

    public String getSslTruststorePassword()
    {
        return sslTruststorePassword;
    }

    @Config("opensearch.ssl.truststore.password")
    @ConfigDescription("SSL truststore password")
    @ConfigSecuritySensitive
    public OpenSearchConfig setSslTruststorePassword(String sslTruststorePassword)
    {
        this.sslTruststorePassword = sslTruststorePassword;
        return this;
    }

    public boolean isSslSkipCertificateValidation()
    {
        return sslSkipCertificateValidation;
    }

    @Config("opensearch.ssl.skip-certificate-validation")
    @ConfigDescription("Skip SSL certificate validation (including expiration checks). Use only for testing with expired certificates.")
    public OpenSearchConfig setSslSkipCertificateValidation(boolean sslSkipCertificateValidation)
    {
        this.sslSkipCertificateValidation = sslSkipCertificateValidation;
        return this;
    }

    public boolean isVectorSearchEnabled()
    {
        return vectorSearchEnabled;
    }

    @Config("opensearch.vector-search.enabled")
    @ConfigDescription("Enable vector search support")
    public OpenSearchConfig setVectorSearchEnabled(boolean vectorSearchEnabled)
    {
        this.vectorSearchEnabled = vectorSearchEnabled;
        return this;
    }

    public int getVectorSearchDefaultK()
    {
        return vectorSearchDefaultK;
    }

    @Config("opensearch.vector-search.default-k")
    @ConfigDescription("Default k for vector search")
    public OpenSearchConfig setVectorSearchDefaultK(int vectorSearchDefaultK)
    {
        this.vectorSearchDefaultK = vectorSearchDefaultK;
        return this;
    }

    public String getVectorSearchDefaultSpaceType()
    {
        return vectorSearchDefaultSpaceType;
    }

    @Config("opensearch.vector-search.default-space-type")
    @ConfigDescription("Default distance metric for vector search (cosine, l2, inner_product)")
    public OpenSearchConfig setVectorSearchDefaultSpaceType(String vectorSearchDefaultSpaceType)
    {
        this.vectorSearchDefaultSpaceType = vectorSearchDefaultSpaceType;
        return this;
    }

    public int getVectorSearchDefaultEfSearch()
    {
        return vectorSearchDefaultEfSearch;
    }

    @Config("opensearch.vector-search.default-ef-search")
    @ConfigDescription("Default ef_search parameter for HNSW algorithm (higher = better accuracy, slower)")
    public OpenSearchConfig setVectorSearchDefaultEfSearch(int vectorSearchDefaultEfSearch)
    {
        this.vectorSearchDefaultEfSearch = vectorSearchDefaultEfSearch;
        return this;
    }

    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("opensearch.scroll.size")
    @ConfigDescription("Scroll batch size")
    public OpenSearchConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    public String getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("opensearch.scroll.timeout")
    @ConfigDescription("Scroll timeout")
    public OpenSearchConfig setScrollTimeout(String scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("opensearch.max-connections")
    @ConfigDescription("Maximum number of connections")
    public OpenSearchConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("opensearch.connection-timeout")
    @ConfigDescription("Connection timeout in milliseconds")
    public OpenSearchConfig setConnectionTimeout(int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public int getConnectTimeout()
    {
        return connectionTimeout;
    }

    public int getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("opensearch.socket-timeout")
    @ConfigDescription("Socket timeout in milliseconds")
    public OpenSearchConfig setSocketTimeout(int socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public int getMaxRetryTimeout()
    {
        return socketTimeout * 3;
    }

    public String getSchemaPattern()
    {
        return schemaPattern;
    }

    @Config("opensearch.schema-pattern")
    @ConfigDescription("Index pattern for schema discovery")
    public OpenSearchConfig setSchemaPattern(String schemaPattern)
    {
        this.schemaPattern = schemaPattern;
        return this;
    }

    public boolean isHideSystemIndices()
    {
        return hideSystemIndices;
    }

    @Config("opensearch.hide-system-indices")
    @ConfigDescription("Hide system indices from schema")
    public OpenSearchConfig setHideSystemIndices(boolean hideSystemIndices)
    {
        this.hideSystemIndices = hideSystemIndices;
        return this;
    }

    public int getMaxResultWindow()
    {
        return maxResultWindow;
    }

    @Config("opensearch.max-result-window")
    @ConfigDescription("Maximum result window size")
    public OpenSearchConfig setMaxResultWindow(int maxResultWindow)
    {
        this.maxResultWindow = maxResultWindow;
        return this;
    }

    // Nested field configuration getters and setters

    public boolean isNestedFieldAccessEnabled()
    {
        return nestedFieldAccessEnabled;
    }

    @Config("opensearch.nested.enabled")
    @ConfigDescription("Enable nested field access with dot notation (default: true)")
    public OpenSearchConfig setNestedFieldAccessEnabled(boolean nestedFieldAccessEnabled)
    {
        this.nestedFieldAccessEnabled = nestedFieldAccessEnabled;
        return this;
    }

    public int getNestedMaxDepth()
    {
        return nestedMaxDepth;
    }

    @Config("opensearch.nested.max-depth")
    @ConfigDescription("Maximum depth for nested field discovery (default: 5)")
    public OpenSearchConfig setNestedMaxDepth(int nestedMaxDepth)
    {
        this.nestedMaxDepth = nestedMaxDepth;
        return this;
    }

    public boolean isNestedOptimizeQueries()
    {
        return nestedOptimizeQueries;
    }

    @Config("opensearch.nested.optimize-queries")
    @ConfigDescription("Optimize nested queries by grouping predicates (default: true)")
    public OpenSearchConfig setNestedOptimizeQueries(boolean nestedOptimizeQueries)
    {
        this.nestedOptimizeQueries = nestedOptimizeQueries;
        return this;
    }

    public boolean isNestedDiscoverDynamicFields()
    {
        return nestedDiscoverDynamicFields;
    }

    @Config("opensearch.nested.discover-dynamic-fields")
    @ConfigDescription("Discover dynamic fields not in mapping (default: false, for safety)")
    public OpenSearchConfig setNestedDiscoverDynamicFields(boolean nestedDiscoverDynamicFields)
    {
        this.nestedDiscoverDynamicFields = nestedDiscoverDynamicFields;
        return this;
    }

    public boolean isNestedLogMissingFields()
    {
        return nestedLogMissingFields;
    }

    @Config("opensearch.nested.log-missing-fields")
    @ConfigDescription("Log warnings when nested fields are missing in documents (default: false)")
    public OpenSearchConfig setNestedLogMissingFields(boolean nestedLogMissingFields)
    {
        this.nestedLogMissingFields = nestedLogMissingFields;
        return this;
    }

    public boolean isHideDocumentIdColumn()
    {
        return hideDocumentIdColumn;
    }

    @Config("opensearch.hide-document-id-column")
    @ConfigDescription("Hide the _id column from table schema (default: false)")
    public OpenSearchConfig setHideDocumentIdColumn(boolean hideDocumentIdColumn)
    {
        this.hideDocumentIdColumn = hideDocumentIdColumn;
        return this;
    }
}
