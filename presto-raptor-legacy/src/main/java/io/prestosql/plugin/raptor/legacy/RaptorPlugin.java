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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class RaptorPlugin
        implements Plugin
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;

    public RaptorPlugin()
    {
        this(getPluginInfo());
    }

    private RaptorPlugin(PluginInfo info)
    {
        this(info.getName(), info.getMetadataModule(), info.getBackupProviders());
    }

    public RaptorPlugin(String name, Module metadataModule, Map<String, Module> backupProviders)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = requireNonNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(requireNonNull(backupProviders, "backupProviders is null"));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RaptorConnectorFactory(name, metadataModule, backupProviders));
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = RaptorPlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }
}
