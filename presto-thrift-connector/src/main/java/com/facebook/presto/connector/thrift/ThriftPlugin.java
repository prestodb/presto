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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

import java.util.List;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ThriftPlugin
        implements Plugin
{
    private final String name;
    private final Module locationModule;

    public ThriftPlugin()
    {
        this(getPluginInfo());
    }

    private ThriftPlugin(ThriftPluginInfo info)
    {
        this(info.getName(), info.getLocationModule());
    }

    public ThriftPlugin(String name, Module locationModule)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.locationModule = requireNonNull(locationModule, "locationModule is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ThriftConnectorFactory(name, locationModule));
    }

    private static ThriftPluginInfo getPluginInfo()
    {
        ClassLoader classLoader = ThriftPlugin.class.getClassLoader();
        ServiceLoader<ThriftPluginInfo> loader = ServiceLoader.load(ThriftPluginInfo.class, classLoader);
        List<ThriftPluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new ThriftPluginInfo() : getOnlyElement(list);
    }
}
