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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.functionNamespace.execution.SimpleAddressSqlFunctionExecutorsModule;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class MySqlFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    public static final String NAME = "mysql";

    private static final SqlFunctionHandle.Resolver HANDLE_RESOLVER = new SqlFunctionHandle.Resolver();

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
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new MySqlFunctionNamespaceManagerModule(catalogName),
                    new MySqlConnectionModule(),
                    new SimpleAddressSqlFunctionExecutorsModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(MySqlFunctionNamespaceManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
