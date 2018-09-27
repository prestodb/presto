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
package com.facebook.presto.cost;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isNaN;

public final class PlanCostEstimate
{
    public static PlanCostEstimate zero()
    {
        return new PlanCostEstimate(0, 0, 0, 0);
    }

    private final double cpuCost;
    private final double maxMemory;
    private final double maxMemoryWhenOutputting;
    private final double networkCost;

    public PlanCostEstimate(double cpuCost, double peakMemory, double maxMemoryWhenOutputting, double networkCost)
    {
        checkArgument(isNaN(cpuCost) || cpuCost >= 0, "cpuCost cannot be negative");
        checkArgument(isNaN(peakMemory) || peakMemory >= 0, "maxMemory cannot be negative");
        checkArgument(isNaN(maxMemoryWhenOutputting) || maxMemoryWhenOutputting >= 0, "maxMemoryWhenOutputting cannot be negative");
        checkArgument(isNaN(networkCost) || networkCost >= 0, "networkCost cannot be negative");
        this.cpuCost = cpuCost;
        this.maxMemory = peakMemory;
        this.maxMemoryWhenOutputting = maxMemoryWhenOutputting;
        this.networkCost = networkCost;
    }

    public double getCpuCost()
    {
        return cpuCost;
    }

    public double getMaxMemory()
    {
        return maxMemory;
    }

    public double getMaxMemoryWhenOutputting()
    {
        return maxMemoryWhenOutputting;
    }

    public double getNetworkCost()
    {
        return networkCost;
    }

    public PlanNodeCostEstimate toPlanNodeCostEstimate()
    {
        return new PlanNodeCostEstimate(cpuCost, maxMemory, networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpu", cpuCost)
                .add("memory", maxMemory)
                .add("network", networkCost)
                .toString();
    }
}
