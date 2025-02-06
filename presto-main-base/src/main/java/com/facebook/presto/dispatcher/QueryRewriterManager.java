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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.analyzer.QueryPreparerProvider;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.rewriter.QueryRewriterInput;
import com.facebook.presto.spi.rewriter.QueryRewriterOutput;
import com.facebook.presto.spi.rewriter.QueryRewriterProvider;
import com.facebook.presto.spi.rewriter.QueryRewriterProviderFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_ENABLED;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * To provide a query rewriter plugin, i.e. a plugin that inputs a set of session properties and query and returns the
 * updated session properties and rewritten query.
 * 1) Provide implementation for QueryRewriterProviderFactory
 * 2) Provide implementation for QueryRewriterProvider
 * 3) Implement com.facebook.presto.spi.Plugin and provide implementation for Iterable<QueryRewriterProviderFactory> getQueryRewriterProviderFactory()
 * 4) Finally provide implementation of QueryRewriter, which does actual query rewrite.
 * For example:
 * {@link presto-tests/com.facebook.presto.plugin.rewriter.UpperCasingQueryRewriterPlugin}
 */
public class QueryRewriterManager
{
    private static final Logger log = Logger.get(QueryRewriterManager.class);
    private static final File CONFIG_FILE = new File("etc/query-rewriter.properties");
    private static final String NAME_PROPERTY = "query-rewriter.name";

    private final AtomicReference<Optional<QueryRewriterProviderFactory>> providerFactory = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Optional<QueryRewriterProvider>> provider = new AtomicReference<>(Optional.empty());
    private final EventListenerManager eventListenerManager;
    private final CatalogManager catalogManager;

    @Inject
    public QueryRewriterManager(EventListenerManager eventListenerManager, CatalogManager catalogManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
    }

    public static Boolean isQueryRewriterPluginEnabled(SessionContext sessionContext)
    {
        return Boolean.parseBoolean(sessionContext.getSystemProperties().getOrDefault(IS_QUERY_REWRITER_PLUGIN_ENABLED, "false"));
    }

    public void addQueryRewriterProviderFactory(QueryRewriterProviderFactory queryRewriterProviderFactory)
    {
        requireNonNull(queryRewriterProviderFactory, "queryRewriterProviderFactory is null");
        checkState(providerFactory.compareAndSet(Optional.empty(), Optional.of(queryRewriterProviderFactory)),
                format("A query rewriter factory is already registered with name %s", queryRewriterProviderFactory.getName()));
    }

    public void loadQueryRewriterProvider()
    {
        loadQueryRewriterProvider(ImmutableMap.of());
    }

    @VisibleForTesting
    public void loadQueryRewriterProvider(Map<String, String> config)
    {
        List<EventListener> configuredEventListener = eventListenerManager.getConfiguredEventListeners();
        Optional<QueryRewriterProviderFactory> queryRewriterProviderFactory = providerFactory.get();
        if (queryRewriterProviderFactory.isPresent()) {
            log.info("-- Loading query rewriter --");
            Map<String, String> properties = ImmutableMap.copyOf(config);
            if (properties.isEmpty()) {
                try {
                    File configFileLocation = CONFIG_FILE.getAbsoluteFile();
                    properties = new HashMap<>(loadProperties(configFileLocation));
                    String name = properties.remove(NAME_PROPERTY);
                    checkArgument(!isNullOrEmpty(name),
                            "Query rewriter configuration %s does not contain %s", configFileLocation, NAME_PROPERTY);
                }
                catch (IOException ioException) {
                    log.warn(ioException, "Unable to load %s", CONFIG_FILE);
                }
            }
            QueryRewriterProvider queryRewriterProvider = queryRewriterProviderFactory.get().create(configuredEventListener, ImmutableMap.copyOf(properties));
            checkState(provider.compareAndSet(Optional.empty(), Optional.of(queryRewriterProvider)),
                    format("A query rewriter provider is already registered %s", queryRewriterProvider));
        }
    }

    public Optional<QueryRewriterProvider> getQueryRewriterProvider()
    {
        return provider.get();
    }

    public QueryAndSessionProperties rewriteQueryAndSession(
            String query,
            Session session,
            AnalyzerOptions analyzerOptions,
            AnalyzerProvider analyzerProvider,
            QueryPreparerProvider queryPreparerProvider)
    {
        QueryId queryId = session.getQueryId();
        requireNonNull(query, "expected non null query");
        ImmutableSet.Builder<String> enabledCatalogsSetBuilder = ImmutableSet.builder();
        if (getQueryRewriterProvider().isPresent()) {
            // TODO: Revisit how we discover which catalogs have Opt+ enabled - https://github.ibm.com/lakehouse/tracker/issues/22358
            List<Catalog> catalogNames = catalogManager.getCatalogs();
            for (Catalog catalog : catalogNames) {
                Connector connector = catalog.getConnector(catalog.getConnectorId());
                String connectorType = connector.getClass().getName();
                if (connectorType.startsWith("com.facebook.presto.hive") || connectorType.startsWith("com.facebook.presto.iceberg")) {
                    enabledCatalogsSetBuilder.add(catalog.getCatalogName());
                }
            }
            QueryRewriterInput queryRewriterInput = new QueryRewriterInput.Builder()
                    .setQuery(query)
                    .setQueryId(queryId.getId())
                    .setCatalog(session.getCatalog())
                    .setSchema(session.getSchema())
                    .setEnabledCatalogs(enabledCatalogsSetBuilder.build())
                    .setPreparedStatements(session.getPreparedStatements())
                    .setWarningCollector(session.getWarningCollector())
                    .setSessionProperties(session.getSystemProperties())
                    .setAnalyzerOptions(analyzerOptions)
                    .setAnalyzerProvider(analyzerProvider)
                    .setQueryPreparer(queryPreparerProvider.getQueryPreparer())
                    .build();
            QueryRewriterProvider provider = getQueryRewriterProvider().get();
            QueryRewriterOutput queryRewriterOutput = provider.getQueryRewriter().rewriteSQL(queryRewriterInput);
            String rewrittenQuery = queryRewriterOutput.getRewrittenQuery();
            // apply updated session properties.
            Map<String, String> systemPropertyOverrides = queryRewriterOutput.getSessionProperties();
            log.info("createQueryInternal :: QueryId [%s] - Replacing with optimized query", queryId.getId());
            return new QueryAndSessionProperties(Optional.ofNullable(rewrittenQuery), systemPropertyOverrides, queryRewriterOutput.getRuntimeStats());
        }
        return new QueryAndSessionProperties(Optional.empty(), session.getSystemProperties(), new RuntimeStats());
    }
}
