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

import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

/**
 * Guice module for OpenSearch connector dependency injection.
 */
public class OpenSearchModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(OpenSearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchClient.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(NestedValueExtractor.class).in(Scopes.SINGLETON);
        binder.bind(NestedFieldMapper.class).in(Scopes.SINGLETON);
        binder.bind(QueryBuilder.class).in(Scopes.SINGLETON);
        binder.bind(VectorSearchHandler.class).in(Scopes.SINGLETON);

        // Register table functions using Multibinder
        newSetBinder(binder, ConnectorTableFunction.class)
                .addBinding()
                .to(KnnSearchTableFunction.class)
                .in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OpenSearchConfig.class);
    }
}
