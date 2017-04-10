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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.base.Throwables;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private final List<ResourceGroupSpec> rootGroups;
    private final List<ResourceGroupSelector> selectors;
    private final Optional<Duration> cpuQuotaPeriodMillis;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, FileResourceGroupConfig config, JsonCodec<ManagerSpec> codec)
    {
        super(memoryPoolManager);
        requireNonNull(config, "config is null");
        requireNonNull(codec, "codec is null");

        ManagerSpec managerSpec;
        try {
            managerSpec = codec.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        this.rootGroups = managerSpec.getRootGroups();
        this.cpuQuotaPeriodMillis = managerSpec.getCpuQuotaPeriod();
        validateRootGroups(managerSpec);
        this.selectors = buildSelectors(managerSpec);
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriodMillis()
    {
        return cpuQuotaPeriodMillis;
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups;
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext context)
    {
        Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry = getMatchingSpec(group, context);
        configureGroup(group, entry.getValue());
    }

    @Override
    public List<ResourceGroupSelector> getSelectors()
    {
        return selectors;
    }
}
