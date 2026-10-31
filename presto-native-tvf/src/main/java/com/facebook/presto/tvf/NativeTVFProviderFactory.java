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
package com.facebook.presto.tvf;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.facebook.presto.spi.tvf.TVFProviderContext;
import com.facebook.presto.spi.tvf.TVFProviderFactory;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Factory class to create instance of {@link NativeTVFProvider}.
 * This factory is registered in
 * {@link TvfPlugin#getTVFProviderFactories()}.
 */
public final class NativeTVFProviderFactory
        implements TVFProviderFactory
{
    protected static final String NAME = "system";

    private static final NativeTableFunctionHandle.Resolver
            HANDLE_RESOLVER = new NativeTableFunctionHandle.Resolver();

    private static final NativeTableFunctionSplit.Resolver
            SPLIT_RESOLVER = new NativeTableFunctionSplit.Resolver();

    /**
     * Gets the factory name.
     *
     * @return the name
     */
    @Override
    public String getName()
    {
        return NAME;
    }

    /**
     * Gets the table function handle resolver.
     *
     * @return the table function handle resolver
     */
    @Override
    public TableFunctionHandleResolver
            getTableFunctionHandleResolver()
    {
        return HANDLE_RESOLVER;
    }

    /**
     * Gets the table function split resolver.
     *
     * @return the table function split resolver
     */
    @Override
    public TableFunctionSplitResolver
            getTableFunctionSplitResolver()
    {
        return SPLIT_RESOLVER;
    }

    /**
     * Creates a TVF provider.
     *
     * @param config the configuration properties
     * @param context the TVF provider context
     * @return the TVF provider instance
     */
    @Override
    public TVFProvider createTVFProvider(
            final Map<String, String> config,
            final TVFProviderContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new NativeTVFProviderModule(
                            context.getNodeManager(),
                            context.getTypeManager()),
                    new NativeWorkerCommunicationModule(
                            context.getAuthClientConfigs()));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            Key<HttpClient> httpClientKey = Key.get(
                    HttpClient.class,
                    ForWorkerInfo.class);
            HttpClientHolder.setHttpClient(
                    injector.getInstance(httpClientKey));

            return injector.getInstance(NativeTVFProvider.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
