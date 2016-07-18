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
package com.facebook.presto.atop;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.node.NodeConfig;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public class AtopPlugin
        implements Plugin
{
    private TypeManager typeManager;
    private NodeManager nodeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        NodeConfig nodeConfig = new ConfigurationFactory(optionalConfig).build(NodeConfig.class);
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new AtopConnectorFactory(AtopProcessFactory.class, optionalConfig, getClassLoader(), typeManager, nodeManager, nodeConfig)));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), AtopPlugin.class.getClassLoader());
    }
}
