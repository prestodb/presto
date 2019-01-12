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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;

public final class PlanNodeCostEstimate
{
    private static final PlanNodeCostEstimate INFINITE = new PlanNodeCostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);
    private static final PlanNodeCostEstimate UNKNOWN = new PlanNodeCostEstimate(NaN, NaN, NaN);
    private static final PlanNodeCostEstimate ZERO = new PlanNodeCostEstimate(0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    public static PlanNodeCostEstimate infinite()
    {
        return INFINITE;
    }

    public static PlanNodeCostEstimate unknown()
    {
        return UNKNOWN;
    }

    public static PlanNodeCostEstimate zero()
    {
        return ZERO;
    }

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

    @JsonCreator
    public PlanNodeCostEstimate(
            @JsonProperty("cpuCost") double cpuCost,
            @JsonProperty("memoryCost") double memoryCost,
            @JsonProperty("networkCost") double networkCost)
    {
        checkArgument(isNaN(cpuCost) || cpuCost >= 0, "cpuCost cannot be negative");
        checkArgument(isNaN(memoryCost) || memoryCost >= 0, "memoryCost cannot be negative");
        checkArgument(isNaN(networkCost) || networkCost >= 0, "networkCost cannot be negative");
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    /**
     * Returns CPU component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getCpuCost()
    {
        return cpuCost;
    }

    /**
     * Returns memory component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getMemoryCost()
    {
        return memoryCost;
    }

    /**
     * Returns network component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getNetworkCost()
    {
        return networkCost;
    }

    /**
     * Returns true if this cost has unknown components.
     */
    public boolean hasUnknownComponents()
    {
        return isNaN(cpuCost) || isNaN(memoryCost) || isNaN(networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
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
        return Double.compare(that.cpuCost, cpuCost) == 0 &&
                Double.compare(that.memoryCost, memoryCost) == 0 &&
                Double.compare(that.networkCost, networkCost) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuCost, memoryCost, networkCost);
    }

    public PlanNodeCostEstimate add(PlanNodeCostEstimate other)
    {
        return new PlanNodeCostEstimate(
                cpuCost + other.cpuCost,
                memoryCost + other.memoryCost,
                networkCost + other.networkCost);
    }
}
