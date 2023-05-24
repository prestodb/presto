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
package com.facebook.presto.resourcemanager;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.PartitionedSplitsInfo;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class NodeTaskState
{
    private final PartitionedSplitsInfo partitionedSplitsInfo;
    private final long totalMemoryUsageInBytes;
    private final double cpuUtilizationPercentage;

    @ThriftConstructor
    public NodeTaskState(PartitionedSplitsInfo partitionedSplitsInfo, long totalMemoryUsageInBytes, double cpuUtilizationPercentage)
    {
        this.partitionedSplitsInfo = requireNonNull(partitionedSplitsInfo, "partitionedSplitsInfo is null");
        this.totalMemoryUsageInBytes = totalMemoryUsageInBytes;
        this.cpuUtilizationPercentage = cpuUtilizationPercentage;
    }

    @ThriftField(1)
    public PartitionedSplitsInfo getPartitionedSplitsInfo()
    {
        return partitionedSplitsInfo;
    }

    @ThriftField(2)
    public long getTotalMemoryUsageInBytes()
    {
        return totalMemoryUsageInBytes;
    }

    @ThriftField(3)
    public double getCpuUtilizationPercentage()
    {
        return cpuUtilizationPercentage;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeTaskState that = (NodeTaskState) o;
        return totalMemoryUsageInBytes == that.totalMemoryUsageInBytes && Double.compare(that.cpuUtilizationPercentage, cpuUtilizationPercentage) == 0 && partitionedSplitsInfo.equals(that.partitionedSplitsInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionedSplitsInfo, totalMemoryUsageInBytes, cpuUtilizationPercentage);
    }
}
