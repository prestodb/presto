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
package io.prestosql.execution.resourceGroups;

import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerContext;

import static java.util.Objects.requireNonNull;

public class ResourceGroupConfigurationManagerContextInstance
        implements ResourceGroupConfigurationManagerContext
{
    private final ClusterMemoryPoolManager memoryPoolManager;
    private final String environment;

    public ResourceGroupConfigurationManagerContextInstance(ClusterMemoryPoolManager memoryPoolManager, String environment)
    {
        this.memoryPoolManager = requireNonNull(memoryPoolManager, "memoryPoolManager is null");
        this.environment = requireNonNull(environment, "environment is null");
    }

    @Override
    public ClusterMemoryPoolManager getMemoryPoolManager()
    {
        return memoryPoolManager;
    }

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}
