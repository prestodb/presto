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

package com.facebook.presto.spi.rewriter;

import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.analyzer.QueryPreparer;
import com.facebook.presto.spi.security.Identity;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class QueryRewriterInput
{
    private final String queryId;
    private final Identity identity;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String query;
    private final Set<String> enabledCatalogs;
    private final Map<String, String> sessionProperties;
    private final WarningCollector warningCollector;
    private final Map<String, String> preparedStatements;
    private final AnalyzerProvider analyzerProvider;
    private final QueryPreparer queryPreparer;
    private final AnalyzerOptions analyzerOptions;

    private QueryRewriterInput(
            String queryId,
            Identity identity,
            Optional<String> catalog,
            Optional<String> schema,
            String query,
            Set<String> enabledCatalogs,
            Map<String, String> sessionProperties,
            WarningCollector warningCollector,
            Map<String, String> preparedStatements,
            AnalyzerProvider analyzerProvider,
            AnalyzerOptions analyzerOptions,
            QueryPreparer queryPreparer)
    {
        this.queryId = queryId;
        this.identity = identity;
        this.catalog = catalog;
        this.schema = schema;
        this.query = query;
        this.enabledCatalogs = enabledCatalogs;
        this.sessionProperties = sessionProperties;
        this.warningCollector = warningCollector;
        this.preparedStatements = preparedStatements;
        this.analyzerOptions = analyzerOptions;
        this.analyzerProvider = analyzerProvider;
        this.queryPreparer = queryPreparer;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getQuery()
    {
        return query;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public AnalyzerProvider getAnalyzerProvider()
    {
        return analyzerProvider;
    }

    public AnalyzerOptions getAnalyzerOptions()
    {
        return analyzerOptions;
    }

    public QueryPreparer getQueryPreparer()
    {
        return queryPreparer;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Set<String> getEnabledCatalogs()
    {
        return enabledCatalogs;
    }

    public static class Builder
    {
        private String queryId;
        private Identity identity;
        private Optional<String> catalog = Optional.empty();
        private Optional<String> schema = Optional.empty();
        private String query;
        private Set<String> enabledCatalogs;
        private Map<String, String> sessionProperties = emptyMap();
        private WarningCollector warningCollector = WarningCollector.NOOP;
        private Map<String, String> preparedStatements = emptyMap();
        private AnalyzerProvider analyzerProvider;
        private AnalyzerOptions analyzerOptions;
        private QueryPreparer queryPreparer;

        public Builder setQueryId(String queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public Builder setCatalog(Optional<String> catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder setSchema(Optional<String> schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder setQuery(String query)
        {
            this.query = requireNonNull(query, "query is null");
            return this;
        }

        public Builder setSessionProperties(Map<String, String> sessionProperties)
        {
            this.sessionProperties = sessionProperties;
            return this;
        }

        public Builder setWarningCollector(WarningCollector warningCollector)
        {
            this.warningCollector = warningCollector;
            return this;
        }

        public Builder setPreparedStatements(Map<String, String> preparedStatements)
        {
            this.preparedStatements = preparedStatements;
            return this;
        }

        public Builder setAnalyzerProvider(AnalyzerProvider analyzerProvider)
        {
            this.analyzerProvider = analyzerProvider;
            return this;
        }

        public Builder setAnalyzerOptions(AnalyzerOptions analyzerOptions)
        {
            this.analyzerOptions = analyzerOptions;
            return this;
        }

        public Builder setQueryPreparer(QueryPreparer queryPreparer)
        {
            this.queryPreparer = queryPreparer;
            return this;
        }

        public Builder setIdentity(Identity identity)
        {
            this.identity = identity;
            return this;
        }

        public Builder setEnabledCatalogs(Set<String> enabledCatalogs)
        {
            this.enabledCatalogs = enabledCatalogs;
            return this;
        }

        public QueryRewriterInput build()
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(query, "query is null");
            checkArgument(!query.isEmpty() && !queryId.isEmpty());
            return new QueryRewriterInput(
                    queryId,
                    identity,
                    catalog,
                    schema,
                    query,
                    enabledCatalogs,
                    sessionProperties,
                    warningCollector,
                    preparedStatements,
                    analyzerProvider,
                    analyzerOptions,
                    queryPreparer);
        }
    }
}
