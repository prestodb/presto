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
package io.prestosql.plugin.resourcegroups;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;

import static java.util.Objects.requireNonNull;

public class TestingResourceGroup
        implements ResourceGroup
{
    private final ResourceGroupId id;
    private DataSize memoryLimit;
    private Duration softCpuLimit;
    private Duration hardCpuLimit;
    private long quotaGenerationRate;
    private int softConcurrencyLimit;
    private int hardConcurrencyLimit;
    private int maxQueued;
    private int schedulingWeight;
    private SchedulingPolicy policy;
    private boolean jmxExport;

    public TestingResourceGroup(ResourceGroupId id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public ResourceGroupId getId()
    {
        return id;
    }

    @Override
    public DataSize getSoftMemoryLimit()
    {
        return memoryLimit;
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        memoryLimit = limit;
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        return softCpuLimit;
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        softCpuLimit = limit;
    }

    @Override
    public Duration getHardCpuLimit()
    {
        return hardCpuLimit;
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        hardCpuLimit = limit;
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        return quotaGenerationRate;
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        quotaGenerationRate = rate;
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        return softConcurrencyLimit;
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        this.softConcurrencyLimit = softConcurrencyLimit;
    }

    @Override
    public int getHardConcurrencyLimit()
    {
        return hardConcurrencyLimit;
    }

    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        this.hardConcurrencyLimit = hardConcurrencyLimit;
    }

    @Override
    public int getMaxQueuedQueries()
    {
        return maxQueued;
    }

    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        maxQueued = maxQueuedQueries;
    }

    @Override
    public int getSchedulingWeight()
    {
        return schedulingWeight;
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        schedulingWeight = weight;
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        return policy;
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        this.policy = policy;
    }

    @Override
    public boolean getJmxExport()
    {
        return jmxExport;
    }

    @Override
    public void setJmxExport(boolean export)
    {
        jmxExport = export;
    }
}
