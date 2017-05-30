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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.facebook.presto.raptorx.util.DatabaseUtil.requireParameterNames;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class RaptorPlugin
        implements Plugin
{
    static {
        requireParameterNames();
    }

    private final String name;
    private final Module databaseModule;
    private final Map<String, Module> chunkStoreProviders;

    public RaptorPlugin()
    {
        this(getPluginInfo());
    }

    private RaptorPlugin(PluginInfo info)
    {
        this(info.getName(), info.getDatabaseModule(), info.getChunkStoreProviders());
    }

    public RaptorPlugin(String name, Module databaseModule, Map<String, Module> chunkStoreProviders)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.databaseModule = requireNonNull(databaseModule, "databaseModule is null");
        this.chunkStoreProviders = ImmutableMap.copyOf(requireNonNull(chunkStoreProviders, "chunkStoreProviders is null"));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RaptorConnectorFactory(name, databaseModule, chunkStoreProviders));
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = RaptorPlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }
}
