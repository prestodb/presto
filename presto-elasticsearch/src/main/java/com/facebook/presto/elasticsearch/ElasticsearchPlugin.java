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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

public class ElasticsearchPlugin
        implements Plugin
{
    private Optional<ConnectorFactory> connectorFactory;

    public ElasticsearchPlugin()
    {
        connectorFactory = Optional.of(new ElasticsearchConnectorFactory());
    }

    public void setConnectorFactory(ConnectorFactory factory)
    {
        connectorFactory = Optional.of(factory);
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(connectorFactory.get());
    }
}
