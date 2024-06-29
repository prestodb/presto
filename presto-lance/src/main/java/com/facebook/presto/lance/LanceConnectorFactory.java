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
package com.facebook.presto.lance;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.inject.Injector;

import java.util.Map;

public class LanceConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "lance";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LanceHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        {
            ClassLoader classLoader = LanceConnectorFactory.class.getClassLoader();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                Bootstrap app = new Bootstrap(
                        new LanceModule(),
                        binder -> {
                            binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                            binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                            binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                            binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                            binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                            binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                            binder.bind(FilterStatsCalculatorService.class).toInstance(context.getFilterStatsCalculatorService());
                        });

                Injector injector = app
                        .doNotInitializeLogging()
                        .setRequiredConfigurationProperties(config)
                        .initialize();

                return injector.getInstance(LanceConnector.class);
            }
        }
    }
}
