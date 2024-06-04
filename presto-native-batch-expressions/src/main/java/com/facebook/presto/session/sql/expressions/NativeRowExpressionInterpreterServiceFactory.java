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
package com.facebook.presto.session.sql.expressions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterService;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterServiceFactory;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NativeRowExpressionInterpreterServiceFactory
        implements RowExpressionInterpreterServiceFactory
{
    private final ClassLoader classLoader;

    public NativeRowExpressionInterpreterServiceFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public RowExpressionInterpreterService createInterpreter(Map<String, String> config, NodeManager nodeManager, RowExpressionSerde rowExpressionSerde)
    {
        requireNonNull(nodeManager, "nodeManager is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    binder -> {
                        binder.bind(NodeManager.class).toInstance(nodeManager);
                        binder.bind(RowExpressionSerde.class).toInstance(rowExpressionSerde);
                    },
                    new NativeExpressionModule());

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(NativeBatchRowExpressionInterpreterProvider.class).createInterpreter();
        }
    }

    @Override
    public String getName()
    {
        return "native";
    }
}
