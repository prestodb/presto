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
package com.facebook.presto.sidecar.expressions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.sidecar.NativeSidecarCommunicationModule;
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
    public static final String NAME = "native";

    private final ClassLoader classLoader;

    @Override
    public String getName()
    {
        return NAME;
    }

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
                    new NativeSidecarCommunicationModule(),
                    new NativeExpressionsModule(context.getNodeManager(), context.getRowExpressionSerde(), context.getFunctionMetadataManager(), context.getFunctionResolution()));

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(NativeExpressionOptimizer.class);
        }
    }
}
