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
package com.facebook.presto.execution.resourceGroups;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedSubtract;

@Immutable
class ResourceUsage
{
    private final long cpuUsageMillis;
    private final long memoryUsageBytes;

    public ResourceUsage(long cpuUsageMillis, long memoryUsageBytes)
    {
        this.cpuUsageMillis = cpuUsageMillis;
        this.memoryUsageBytes = memoryUsageBytes;
    }

    public ResourceUsage clone()
    {
        return new ResourceUsage(cpuUsageMillis, memoryUsageBytes);
    }

    public ResourceUsage add(ResourceUsage other)
    {
        long newCpuUsageMillis = saturatedAdd(this.cpuUsageMillis, other.cpuUsageMillis);
        long newMemoryUsageBytes = saturatedAdd(this.memoryUsageBytes, other.memoryUsageBytes);
        return new ResourceUsage(newCpuUsageMillis, newMemoryUsageBytes);
    }

    public ResourceUsage subtract(ResourceUsage other)
    {
        long newCpuUsageMillis = saturatedSubtract(this.cpuUsageMillis, other.cpuUsageMillis);
        long newMemoryUsageBytes = saturatedSubtract(this.memoryUsageBytes, other.memoryUsageBytes);
        return new ResourceUsage(newCpuUsageMillis, newMemoryUsageBytes);
    }

    public long getCpuUsageMillis()
    {
        return cpuUsageMillis;
    }

    public long getMemoryUsageBytes()
    {
        return memoryUsageBytes;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if ((other == null) || (getClass() != other.getClass())) {
            return false;
        }

        ResourceUsage otherUsage = (ResourceUsage) other;
        return cpuUsageMillis == otherUsage.cpuUsageMillis
                && memoryUsageBytes == otherUsage.memoryUsageBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuUsageMillis, memoryUsageBytes);
    }
}
