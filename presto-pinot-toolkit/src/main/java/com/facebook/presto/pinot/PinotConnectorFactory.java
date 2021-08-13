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
package com.facebook.presto.pinot;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class PinotConnectorFactory
        implements ConnectorFactory
{
    public PinotConnectorFactory()
    {
    }

    @Override
    public String getName()
    {
        return "pinot";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PinotHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MBeanModule(),
                    new PinotModule(connectorId), binder -> {
                binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
                binder.bind(ConnectorId.class).toInstance(new ConnectorId(connectorId));
                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                binder.bind(LogicalRowExpressions.class).toInstance(new LogicalRowExpressions(context.getRowExpressionService().getDeterminismEvaluator(), context.getStandardFunctionResolution(), context.getFunctionMetadataManager()));
                binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                binder.bind(PinotMetrics.class).in(Scopes.SINGLETON);
                newExporter(binder).export(PinotMetrics.class).as(generatedNameOf(PinotMetrics.class, connectorId));
                binder.bind(ConnectorNodePartitioningProvider.class).to(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
            });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(PinotConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
