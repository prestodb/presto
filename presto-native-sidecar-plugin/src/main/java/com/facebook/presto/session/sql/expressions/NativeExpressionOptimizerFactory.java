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
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerContext;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NativeExpressionOptimizerFactory
        implements ExpressionOptimizerFactory
{
    private final ClassLoader classLoader;

    public NativeExpressionOptimizerFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public ExpressionOptimizer createOptimizer(Map<String, String> config, ExpressionOptimizerContext context)
    {
        requireNonNull(context, "context is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new NativeExpressionsCommunicationModule(),
                    new NativeExpressionsModule(context.getNodeManager(), context.getRowExpressionSerde(), context.getFunctionMetadataManager(), context.getFunctionResolution()));

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(NativeExpressionOptimizerProvider.class).createOptimizer();
        }
    }

    @Override
    public String getName()
    {
        return "native";
    }
}
