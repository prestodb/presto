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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;

public final class PlanNodeCostEstimate
{
    public static final PlanNodeCostEstimate INFINITE_COST = new PlanNodeCostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);
    public static final PlanNodeCostEstimate UNKNOWN_COST = new PlanNodeCostEstimate(NaN, NaN, NaN);
    public static final PlanNodeCostEstimate ZERO_COST = new PlanNodeCostEstimate(0, 0, 0);

    public static PlanNodeCostEstimate cpuCost(double cpuCost)
    {
        return new PlanNodeCostEstimate(cpuCost, 0, 0);
    }

    public static PlanNodeCostEstimate memoryCost(double memoryCost)
    {
        return new PlanNodeCostEstimate(0, memoryCost, 0);
    }

    public static PlanNodeCostEstimate networkCost(double networkCost)
    {
        return new PlanNodeCostEstimate(0, 0, networkCost);
    }

    private final double individualCpuCost;
    private final double individualMemoryCost;
    private final double individualNetworkCost;
    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    /**
     * Builds a PlanNodeCostEstimate using the individual costs for the cumulative costs
     * PlanNodeCostEstimate becomes cumulative when the add() method is used
     * @param individualCpuCost: the initial, local CPU cost of the PlanNode
     * @param individualMemoryCost: the initial, local memory cost of the PlanNode
     * @param individualNetworkCost: the initial, local network cost of the PlanNode
     */
    public PlanNodeCostEstimate(double individualCpuCost, double individualMemoryCost, double individualNetworkCost)
    {
        this(individualCpuCost, individualMemoryCost, individualNetworkCost, individualCpuCost, individualMemoryCost, individualNetworkCost);
    }

    private PlanNodeCostEstimate(double individualCpuCost, double individualMemoryCost, double individualNetworkCost,
            double cpuCost, double memoryCost, double networkCost)
    {
        checkArgument(isNaN(individualCpuCost) || individualCpuCost >= 0,
                "individualCpuCost cannot be negative");
        checkArgument(isNaN(individualMemoryCost) || individualMemoryCost >= 0,
                "individualMemoryCost cannot be negative");
        checkArgument(isNaN(individualNetworkCost) || individualNetworkCost >= 0,
                "individualNetworkCost cannot be negative");
        checkArgument(isNaN(cpuCost) || cpuCost >= 0,
                "cpuCost cannot be negative");
        checkArgument(isNaN(memoryCost) || memoryCost >= 0,
                "memoryCost cannot be negative");
        checkArgument(isNaN(networkCost) || networkCost >= 0,
                "networkCost cannot be negative");

        this.individualCpuCost = individualCpuCost;
        this.individualMemoryCost = individualMemoryCost;
        this.individualNetworkCost = individualNetworkCost;
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    /**
     * Returns individual CPU component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getIndividualCpuCost()
    {
        return individualCpuCost;
    }

    /**
     * Returns cumulative CPU component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getCpuCost()
    {
        return cpuCost;
    }

    /**
     * Returns individual memory component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getIndividualMemoryCost()
    {
        return individualMemoryCost;
    }

    /**
     * Returns cumulative memory component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getMemoryCost()
    {
        return memoryCost;
    }

    /**
     * Returns individual network component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getIndividualNetworkCost()
    {
        return individualNetworkCost;
    }

    /**
     * Returns cumulative network component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getNetworkCost()
    {
        return networkCost;
    }

    /**
     * Returns true if this cost has unknown components.
     */
    public boolean hasUnknownComponents()
    {
        return isNaN(individualCpuCost) || isNaN(individualMemoryCost) || isNaN(individualNetworkCost) ||
                isNaN(cpuCost) || isNaN(memoryCost) || isNaN(networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("individualCpu", individualCpuCost)
                .add("individualMemory", individualMemoryCost)
                .add("individualNetwork", individualNetworkCost)
                .add("cpu", cpuCost)
                .add("memory", memoryCost)
                .add("network", networkCost)
                .toString();
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
        PlanNodeCostEstimate that = (PlanNodeCostEstimate) o;
        return Double.compare(that.individualCpuCost, individualCpuCost) == 0 &&
                Double.compare(that.individualMemoryCost, individualMemoryCost) == 0 &&
                Double.compare(that.individualNetworkCost, individualNetworkCost) == 0 &&
                Double.compare(that.cpuCost, cpuCost) == 0 &&
                Double.compare(that.memoryCost, memoryCost) == 0 &&
                Double.compare(that.networkCost, networkCost) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(individualCpuCost, individualMemoryCost, individualNetworkCost,
                cpuCost, memoryCost, networkCost);
    }

    public PlanNodeCostEstimate add(PlanNodeCostEstimate other)
    {
        return new PlanNodeCostEstimate(
                individualCpuCost,
                individualMemoryCost,
                individualNetworkCost,
                cpuCost + other.cpuCost,
                memoryCost + other.memoryCost,
                networkCost + other.networkCost);
    }
}
