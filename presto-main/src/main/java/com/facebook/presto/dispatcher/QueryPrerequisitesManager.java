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
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.prerequisites.QueryPrerequisites;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesContext;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryPrerequisitesManager
        implements QueryPrerequisites
{
    private static final Logger log = Logger.get(QueryPrerequisitesManager.class);

    private static final File QUERY_PREREQUISITES_CONFIG = new File("etc/query-prerequisites.properties");
    private static final String QUERY_PREREQUISITES_PROPERTY_NAME = "query-prerequisites.factory";
    private final Map<String, QueryPrerequisitesFactory> queryPrerequisitesFactories = new ConcurrentHashMap<>();
    private final QueryPrerequisites defaultQueryPrerequisites = new DefaultQueryPrerequisites();
    private final AtomicReference<QueryPrerequisites> queryPrerequisites = new AtomicReference<>(defaultQueryPrerequisites);

    public void addQueryPrerequisitesFactory(QueryPrerequisitesFactory queryPrerequisitesFactory)
    {
        requireNonNull(queryPrerequisitesFactory, "queryPrerequisitesFactory is null");

        if (queryPrerequisitesFactories.putIfAbsent(queryPrerequisitesFactory.getName(), queryPrerequisitesFactory) != null) {
            throw new IllegalArgumentException(format("Query Prerequisites '%s' is already registered", queryPrerequisitesFactory.getName()));
        }
    }

    public void loadQueryPrerequisites()
            throws Exception
    {
        if (QUERY_PREREQUISITES_CONFIG.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(QUERY_PREREQUISITES_CONFIG));

            String factoryName = properties.remove(QUERY_PREREQUISITES_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(factoryName),
                    "Query Prerequisites configuration %s does not contain %s", QUERY_PREREQUISITES_CONFIG.getAbsoluteFile(), QUERY_PREREQUISITES_PROPERTY_NAME);

            log.info("-- Loading query prerequisites factory --");

            QueryPrerequisitesFactory queryPrerequisitesFactory = queryPrerequisitesFactories.get(factoryName);
            checkState(queryPrerequisitesFactory != null, "Query prerequisites factory %s is not registered", factoryName);

            QueryPrerequisites queryPrerequisites = queryPrerequisitesFactory.create(properties);
            checkState(this.queryPrerequisites.compareAndSet(defaultQueryPrerequisites, queryPrerequisites), "Query prerequisites has already been set");

            log.info("-- Loaded query prerequisites %s --", factoryName);
        }
    }

    @Override
    public CompletableFuture<?> waitForPrerequisites(QueryId queryId, QueryPrerequisitesContext context, WarningCollector warningCollector)
    {
        checkState(queryPrerequisites.get() != null, "Query prerequisites not initiated");
        return queryPrerequisites.get().waitForPrerequisites(queryId, context, warningCollector);
    }

    @Override
    public void queryFinished(QueryId queryId)
    {
        checkState(queryPrerequisites.get() != null, "Query prerequisites not initiated");
        queryPrerequisites.get().queryFinished(queryId);
    }
}
