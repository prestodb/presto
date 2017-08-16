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
package com.facebook.presto.resourceGroups.connector;

import com.facebook.presto.resourceGroups.ResourceGroupConfigurationInfo;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ResourceGroupsConnectorFactory
        implements ConnectorFactory
{
    private static final String name = "resource-group-managers";
    private final ResourceGroupConfigurationInfo configurationInfo;

    public ResourceGroupsConnectorFactory(ResourceGroupConfigurationInfo configurationInfo)
    {
        this.configurationInfo = requireNonNull(configurationInfo, "configurationInfo is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ResourceGroupsHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new ResourceGroupsConnectorModule(),
                    binder -> binder.bind(ResourceGroupConfigurationInfo.class).toInstance(configurationInfo));
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(ResourceGroupsConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
