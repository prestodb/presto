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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerContext;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.RestFunctionHandle;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Factory class to create instance of {@link RestBasedFunctionNamespaceManager}.
 * This factor is registered in {@link FunctionNamespaceManagerPlugin#getFunctionNamespaceManagerFactories()}.
 */
public class RestBasedFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    public static final String NAME = "rest";

    private static final RestFunctionHandle.Resolver HANDLE_RESOLVER = new RestFunctionHandle.Resolver();

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionHandleResolver getHandleResolver()
    {
        return HANDLE_RESOLVER;
    }

    @Override
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new RestBasedCommunicationModule(),
                    new RestBasedFunctionNamespaceManagerModule(catalogName),
                    new RestSqlFunctionExecutorsModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(RestBasedFunctionNamespaceManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
