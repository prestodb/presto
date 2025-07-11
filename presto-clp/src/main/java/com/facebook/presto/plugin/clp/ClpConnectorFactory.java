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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ClpConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "clp";

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ClpHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        try {
            Bootstrap app = new Bootstrap(new JsonModule(), new ClpModule(), binder -> {
                binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
            });

            Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(ClpConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
